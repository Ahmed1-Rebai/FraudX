import {
  ArrowLeft,
  FileSearch,
  Network as NetworkIcon,
  RotateCcw,
  Sparkles,
  Waypoints,
} from "lucide-react";
import { useEffect, useMemo, useState } from "react";
import {
  ApiError,
  api,
  STATUSES,
  type ExplanationView,
  type Investigation,
  type Level,
  type Status,
  type Transaction,
} from "../api";
import { Meter, PrecedentCard, ScoreRing } from "../components/charts";
import { RiskBadge, StatusPill } from "../components/Risk";
import { Topbar } from "../components/Shell";
import { useToast } from "../components/Toast";
import { Card, CardHeader, Empty, ErrorBanner, Segmented, Skeleton, SkeletonRows } from "../components/ui";
import { fmt } from "../lib/format";
import { usePolling } from "../lib/hooks";

type Counts = Partial<Record<Status, number>>;
type Fact = { key: string; label: string; show?: (v: unknown) => string };

const yesNo = (v: unknown) => (v ? "Yes" : "No");
const seconds = (v: unknown) => {
  const s = Number(v);
  return s < 60 ? `${s.toFixed(0)} s` : s < 3600 ? `${(s / 60).toFixed(0)} min` : `${(s / 3600).toFixed(1)} h`;
};

// Evidence keys written by fraudx.explain.evidence, in the order an analyst reads them.
const BEHAVIOUR: Fact[] = [
  { key: "features.amount_to_mean_ratio", label: "Amount vs. usual", show: (v) => `${Number(v).toFixed(1)}×` },
  { key: "features.balance_drain_ratio", label: "Balance drained", show: (v) => fmt.pct(Number(v)) },
  { key: "features.cust_txn_count_5m", label: "Transactions, last 5 min" },
  { key: "features.cust_txn_count_1h", label: "Transactions, last hour" },
  { key: "features.seconds_since_last_txn", label: "Since previous one", show: seconds },
  { key: "features.cust_history_count", label: "Past transactions" },
  { key: "features.is_new_device", label: "New device", show: yesNo },
  { key: "features.is_new_ip", label: "New IP address", show: yesNo },
  { key: "features.is_new_country", label: "New country", show: yesNo },
  { key: "features.device_customer_count", label: "Customers on the device" },
  { key: "features.ip_customer_count", label: "Customers on the IP" },
];
const GRAPH: Fact[] = [
  { key: "graph.distance_to_known_fraud", label: "Hops to confirmed fraud" },
  { key: "graph.fraud_neighbor_count", label: "Confirmed-fraud neighbours" },
  { key: "graph.payer_confirmed_fraud", label: "Payer confirmed as fraud", show: yesNo },
  { key: "graph.is_new_payee", label: "First payment to payee", show: yesNo },
  { key: "graph.payee_payer_count", label: "Accounts paying the payee" },
  { key: "graph.payee_payee_count", label: "Accounts the payee pays" },
  { key: "graph.payer_payee_count", label: "Payees of the payer" },
  { key: "graph.customer_ring_size", label: "Customer's device ring" },
  { key: "graph.payee_owner_ring_size", label: "Payee owner's device ring" },
];
const SIGNALS = [
  { key: "ml", label: "ML prediction", note: "LightGBM on transaction features" },
  { key: "anomaly", label: "Anomaly", note: "Isolation Forest on legitimate traffic" },
  { key: "graph", label: "Graph risk", note: "LightGBM on Neo4j relationship features" },
];
const DONE: Record<Status, string> = {
  CONFIRMED_FRAUD: "Confirmed as fraud",
  FALSE_POSITIVE: "Marked as a false positive",
  DISMISSED: "Case dismissed",
  OPEN: "Case reopened",
};

/** An evidence value as an analyst reads it. */
function value(v: unknown): string {
  if (typeof v === "number") return Number.isInteger(v) ? fmt.int(v) : String(+v.toFixed(4));
  if (typeof v === "boolean") return v ? "yes" : "no";
  return v == null ? "–" : String(v);
}

const generatorLabel = (g: string) =>
  g === "template" ? "deterministic template" : g.startsWith("llm:") ? `Claude (${g.split("/").pop()})` : g;

function FactGrid({ facts, evidence }: { facts: Fact[]; evidence: Record<string, unknown> }) {
  const present = facts.filter((f) => evidence[f.key] != null);
  if (!present.length) return <div className="muted">Not recorded for this transaction.</div>;
  return (
    <div className="stat-grid">
      {present.map((f) => (
        <div key={f.key} className="stat">
          <div className="label">{f.label}</div>
          <div className="value">{f.show ? f.show(evidence[f.key]) : value(evidence[f.key])}</div>
        </div>
      ))}
    </div>
  );
}

function CaseView({ tx, onBack, onChanged }: { tx: string; onBack: () => void; onChanged: () => void }) {
  const toast = useToast();
  const [txn, setTxn] = useState<Transaction | null | undefined>(undefined);
  const [inv, setInv] = useState<Investigation | null | undefined>(undefined);
  const [why, setWhy] = useState<ExplanationView | null>(null);
  const [note, setNote] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [saving, setSaving] = useState<Status | null>(null);

  useEffect(() => {
    let alive = true;
    const missing = (e: unknown) => e instanceof ApiError && e.status === 404;
    api.transaction(tx).then(
      (t) => alive && setTxn(t),
      (e: Error) => alive && (missing(e) ? setTxn(null) : setError(e.message)),
    );
    api.investigation(tx).then(
      (i) => {
        if (!alive) return;
        setInv(i);
        setNote(i.analyst_note ?? "");
      },
      (e: Error) => alive && (missing(e) ? setInv(null) : setError(e.message)),
    );
    api.explanation(tx).then((w) => alive && setWhy(w), () => alive && setWhy(null));
    return () => {
      alive = false;
    };
  }, [tx]);

  const decide = async (status: Status) => {
    setSaving(status);
    try {
      setInv(await api.decide(tx, status, note));
      toast(`${DONE[status]} · ${tx}`);
      onChanged();
    } catch (e) {
      toast(e instanceof Error ? e.message : String(e), "critical");
    } finally {
      setSaving(null);
    }
  };

  if (txn === undefined || inv === undefined) {
    return error ? <ErrorBanner>{error}</ErrorBanner> : (
      <Card><div className="card-body"><Skeleton height={116} /><div style={{ marginTop: 18 }}><SkeletonRows rows={6} /></div></div></Card>
    );
  }
  if (txn === null && inv === null) {
    return <Card><Empty title={`No transaction ${tx}`} icon={FileSearch}>It is not in the history (Cassandra) and has no case.</Empty></Card>;
  }

  const evidence = (inv?.evidence ?? {}) as Record<string, unknown>;
  const score = inv?.risk.score ?? txn?.risk_score ?? null;
  const level: Level | null = inv?.risk.level ?? txn?.risk_level ?? null;
  const rules = inv?.risk.rule_hits ?? txn?.rule_hits ?? [];
  const components: Record<string, number | undefined> = inv?.risk.components ?? {
    ml: txn?.ml_score ?? undefined,
    anomaly: txn?.anomaly_score ?? undefined,
    graph: txn?.graph_score ?? undefined,
  };
  const customer = inv?.customer_id ?? txn!.customer_id;
  const eventTime = inv?.event_time ?? txn!.event_time;
  const amount = txn ? fmt.money(txn.amount, txn.currency)
    : fmt.money(Number(evidence["transaction.amount"]), String(evidence["transaction.currency"] ?? ""));
  const type = txn?.transaction_type ?? String(evidence["transaction.type"] ?? "–");

  return (
    <div className="grid" style={{ gap: 16 }}>
      <Card>
        <div className="case-head">
          <div className="back">
            <button type="button" className="btn ghost" onClick={onBack}><ArrowLeft size={15} /> Queue</button>
          </div>
          {score != null && level ? <ScoreRing score={score} level={level} /> : null}
          <div style={{ flex: 1, minWidth: 180 }}>
            <div className="row" style={{ gap: 10, flexWrap: "wrap" }}>
              <h2 className="mono">{tx}</h2>
              {inv ? <StatusPill status={inv.status} /> : <span className="pill">No case</span>}
            </div>
            <div className="row" style={{ gap: 8, flexWrap: "wrap", marginTop: 10 }}>
              <RiskBadge level={level} />
              {rules.map((r) => <span key={r} className="chip rule">{r}</span>)}
              {inv?.risk.missing_signals.map((s) => <span key={s} className="chip">scored without {s}</span>)}
            </div>
            <dl className="case-facts">
              <div><dt>Amount</dt><dd className="num">{amount}</dd></div>
              <div><dt>Type</dt><dd>{type}</dd></div>
              <div><dt>Customer</dt><dd><a className="mono" href={`#/graph/${encodeURIComponent(customer)}`}>{customer}</a></dd></div>
              <div><dt>Event time (UTC)</dt><dd className="num">{fmt.dateTime(eventTime)}</dd></div>
            </dl>
          </div>
        </div>
        {inv && (
          <div className="case-decision">
            <input className="input" placeholder="Analyst note (optional)" value={note} maxLength={2000}
                   onChange={(e) => setNote(e.target.value)} aria-label="Analyst note" />
            <button type="button" className="btn danger" disabled={saving != null}
                    aria-pressed={inv.status === "CONFIRMED_FRAUD"} onClick={() => decide("CONFIRMED_FRAUD")}>
              <span className="btn-dot danger" /> Confirm fraud
            </button>
            <button type="button" className="btn" disabled={saving != null}
                    aria-pressed={inv.status === "FALSE_POSITIVE"} onClick={() => decide("FALSE_POSITIVE")}>
              <span className="btn-dot good" /> False positive
            </button>
            <button type="button" className="btn ghost" disabled={saving != null}
                    aria-pressed={inv.status === "DISMISSED"} onClick={() => decide("DISMISSED")}>
              <span className="btn-dot neutral" /> Dismiss
            </button>
            {inv.status !== "OPEN" && (
              <button type="button" className="btn ghost" disabled={saving != null} onClick={() => decide("OPEN")}>
                <RotateCcw size={14} /> Reopen
              </button>
            )}
          </div>
        )}
      </Card>

      {inv?.feedback && (
        <div className={`callout row ${inv.feedback.flagged ? "critical" : "good"}`} style={{ gap: 10 }}>
          <Waypoints size={18} aria-hidden style={{ flex: "none" }} />
          <span>
            Payee account <span className="mono">{inv.feedback.account}</span>{" "}
            {inv.feedback.flagged
              ? `is marked as confirmed fraud in the graph (${inv.feedback.confirmations} confirmed case${inv.feedback.confirmations > 1 ? "s" : ""}). New first transfers to it raise the payee_confirmed_fraud rule.`
              : "carries no confirmed-fraud mark in the graph."}
          </span>
        </div>
      )}

      {inv === null ? (
        <div className="callout">
          This transaction scored below the alert threshold, so no case was opened and no explanation was written.
        </div>
      ) : (
        <>
          <Card>
            <CardHeader title="Why it was flagged" icon={Sparkles}
                        hint={why ? `Written by the ${generatorLabel(why.generator)}; every reason cites stored evidence` : undefined} />
            <div className="card-body">
              {!why ? <SkeletonRows rows={4} /> : (
                <>
                  <div className={`callout ${level === "HIGH" ? "critical" : level === "MEDIUM" ? "warning" : ""}`}>{why.summary}</div>
                  <ol className="reasons" style={{ marginTop: 16 }}>
                    {why.reasons.map((r, i) => (
                      <li key={i}>
                        <span className="idx">{i + 1}</span>
                        <div>
                          {r.text}
                          <div className="evidence">
                            {Object.entries(r.evidence).map(([k, v]) => (
                              <span key={k} className="chip"><b>{k}</b>{value(v)}</span>
                            ))}
                          </div>
                        </div>
                      </li>
                    ))}
                  </ol>
                </>
              )}
            </div>
          </Card>

          {((inv?.precedents && inv.precedents.length > 0) || (why?.precedents && why.precedents.length > 0)) && (
            <Card>
              <CardHeader
                title="Case-law precedents"
                icon={FileSearch}
                hint="Historically similar investigations retrieved via MongoDB Vector Cosine Search"
              />
              <div className="card-body">
                <div className="precedents-list">
                  {(inv?.precedents ?? why?.precedents ?? []).map((p) => (
                    <PrecedentCard key={p.transaction_id} precedent={p} />
                  ))}
                </div>
              </div>
            </Card>
          )}
        </>
      )}

      <div className="grid grid-2" style={{ gap: 16 }}>
        <Card>
          <CardHeader title="Signals" hint="Calibrated fraud probabilities behind the score" />
          <div className="card-body" style={{ display: "grid", gap: 14 }}>
            {SIGNALS.map((s) => {
              const share = evidence[`contribution.${s.key}`];
              return (
                <Meter key={s.key} label={s.label} value={components[s.key] ?? null}
                       note={typeof share === "number" ? `${s.note} · ${fmt.pct(share)} of the score` : s.note} />
              );
            })}
          </div>
        </Card>
        <Card>
          <CardHeader title="Transaction" />
          <div className="card-body">
            {!txn ? <div className="muted">Not in the history store yet.</div> : (
              <dl className="kv">
                <dt>From</dt><dd className="mono">{txn.source_account}</dd>
                <dt>To</dt>
                <dd><span className="mono">{txn.destination_account}</span>
                  {evidence["payee.kind"] === "MERCHANT" && <span className="chip" style={{ marginLeft: 8 }}>merchant</span>}</dd>
                <dt>Device</dt><dd className="mono">{txn.device_id ?? "–"}</dd>
                <dt>IP address</dt><dd className="mono">{txn.ip_address ?? "–"}</dd>
                <dt>Location</dt>
                <dd>{[txn.city, txn.country].filter(Boolean).join(", ") || "Not provided"}
                  {evidence["profile.home_country"] != null && <span className="muted"> · home country {String(evidence["profile.home_country"])}</span>}</dd>
                <dt>Seen</dt><dd>{fmt.relative(txn.event_time)}</dd>
              </dl>
            )}
          </div>
        </Card>
      </div>

      {inv && (
        <Card>
          <CardHeader title="Evidence" hint="The facts the decision was made on, as stored with the case (MongoDB)">
            <a className="btn ghost" href={`#/graph/${encodeURIComponent(customer)}`}><NetworkIcon size={15} /> Graph explorer</a>
          </CardHeader>
          <div className="card-body" style={{ display: "grid", gap: 18 }}>
            <div>
              <h3 className="section-title">Customer behaviour</h3>
              <FactGrid facts={BEHAVIOUR} evidence={evidence} />
            </div>
            <div>
              <h3 className="section-title">Graph relationships</h3>
              <FactGrid facts={GRAPH} evidence={evidence} />
            </div>
          </div>
        </Card>
      )}
    </div>
  );
}

export function Investigations({ status, tx, counts, onChanged, go }: {
  status: Status;
  tx?: string;
  counts: Counts | null;
  onChanged: () => void;
  go: (hash: string) => void;
}) {
  const queue = usePolling(() => api.investigations(status), 20_000, [status]);
  const [query, setQuery] = useState("");
  const [order, setOrder] = useState<"newest" | "riskiest">("newest");

  const cases = useMemo(() => {
    const q = query.trim().toLowerCase();
    const shown = (queue.data ?? []).filter(
      (c) => !q || c.transaction_id.toLowerCase().includes(q) || c.customer_id.toLowerCase().includes(q),
    );
    return order === "riskiest" ? [...shown].sort((a, b) => b.risk.score - a.risk.score) : shown;
  }, [queue.data, query, order]);
  const total = counts?.[status];

  return (
    <>
      <Topbar title="Investigations" subtitle="High-risk alerts become cases; analyst decisions feed back into the graph" />
      <div className="content">
        <div className="toolbar">
          <Segmented label="Case status" value={status} onChange={(s) => go(`#/investigations/${s}`)}
                     options={STATUSES.map((s) => ({ value: s, label: fmt.status(s), count: counts?.[s] }))} />
        </div>
        <div className={`grid grid-split${tx ? " has-case" : ""}`}>
          <Card className="queue-card">
            <CardHeader title="Queue" hint={total != null && total > cases.length && !query
              ? `Newest ${cases.length} of ${fmt.int(total)}` : `${fmt.int(cases.length)} case${cases.length === 1 ? "" : "s"}`}>
              <Segmented label="Order" value={order} onChange={setOrder}
                         options={[{ value: "newest", label: "Newest" }, { value: "riskiest", label: "Riskiest" }]} />
            </CardHeader>
            <div style={{ padding: "12px 16px 10px" }}>
              <input className="input" style={{ width: "100%" }} placeholder="Filter by transaction or customer"
                     value={query} onChange={(e) => setQuery(e.target.value)} aria-label="Filter the queue" />
            </div>
            <div className="queue-list">
              {queue.error ? <div style={{ padding: 16 }}><ErrorBanner>{queue.error}</ErrorBanner></div>
                : !queue.data ? <div style={{ padding: "4px 16px" }}><SkeletonRows rows={8} /></div>
                : cases.length === 0 ? <Empty title={query ? "No case matches" : `No ${fmt.status(status).toLowerCase()} cases`}>
                    {status === "OPEN" && !query && "Cases open when a transaction scores HIGH."}
                  </Empty>
                : cases.map((c) => (
                  <a key={c.transaction_id} href={`#/case/${encodeURIComponent(c.transaction_id)}`}
                     className={`queue-item${c.transaction_id === tx ? " selected" : ""}`}
                     aria-current={c.transaction_id === tx ? "true" : undefined}>
                    <span className="mono" style={{ fontWeight: 600, minWidth: 0, overflow: "hidden", textOverflow: "ellipsis" }}>{c.transaction_id}</span>
                    <RiskBadge level={c.risk.level} score={c.risk.score} />
                    <span className="sub">
                      {c.customer_id} · opened {fmt.relative(c.created_at)}
                      {c.risk.rule_hits.length > 0 && ` · ${c.risk.rule_hits.join(", ")}`}
                    </span>
                  </a>
                ))}
            </div>
          </Card>
          {tx ? (
            <CaseView key={tx} tx={tx} onBack={() => go(`#/investigations/${status}`)}
                      onChanged={() => { queue.refresh(); onChanged(); }} />
          ) : (
            <Card>
              <Empty title="Select a case" icon={FileSearch}>
                Pick a case from the queue to see its score, the evidence it rests on, the explanation and the graph around the customer.
              </Empty>
            </Card>
          )}
        </div>
      </div>
    </>
  );
}
