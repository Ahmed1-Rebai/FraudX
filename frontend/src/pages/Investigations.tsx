import { useEffect, useState, type ReactNode } from "react";
import {
  ApiError,
  api,
  fmt,
  type ExplanationView,
  type Investigation,
  type Status,
  type Transaction,
} from "../api";
import { Meter } from "../components/Meter";
import { RiskBadge } from "../components/RiskBadge";

const STATUSES: Status[] = ["OPEN", "CONFIRMED_FRAUD", "FALSE_POSITIVE", "DISMISSED"];
const statusLabel = (s: Status) => s.replace("_", " ").toLowerCase().replace(/^\w/, (c) => c.toUpperCase());
const GRAPH_LABELS: Record<string, string> = {
  "graph.payee_owner_ring_size": "Payee owner's device ring",
  "graph.customer_ring_size": "Customer's device ring",
  "graph.payee_payer_count": "Accounts that paid the payee",
  "graph.payee_payee_count": "Accounts the payee paid",
  "graph.payer_payee_count": "Other accounts the payer paid",
  "graph.is_new_payee": "First payment to this payee",
};

function Section({ title, children }: { title: string; children: ReactNode }) {
  return (
    <section style={{ borderTop: "1px solid var(--grid)", paddingTop: 12, marginTop: 12 }}>
      <h3 style={{ fontSize: 13, margin: "0 0 8px", fontWeight: 600 }}>{title}</h3>
      {children}
    </section>
  );
}

function Fact({ label, children }: { label: string; children: ReactNode }) {
  return (
    <div style={{ display: "grid", gridTemplateColumns: "210px 1fr", gap: 8, padding: "2px 0" }}>
      <span className="secondary">{label}</span>
      <span>{children}</span>
    </div>
  );
}

function Case({ tx, go, onDecided }: { tx: string; go: (h: string) => void; onDecided: () => void }) {
  const [txn, setTxn] = useState<Transaction | null>(null);
  const [inv, setInv] = useState<Investigation | null | undefined>(undefined);
  const [why, setWhy] = useState<ExplanationView | null>(null);
  const [note, setNote] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    setTxn(null);
    setInv(undefined);
    setWhy(null);
    setError(null);
    setNote("");
    api.transaction(tx).then(setTxn, (e: Error) => setError(e.message));
    api.investigation(tx).then(
      (i) => {
        setInv(i);
        setNote(i.analyst_note ?? "");
      },
      (e) => (e instanceof ApiError && e.status === 404 ? setInv(null) : setError(String(e))),
    );
    api.explanation(tx).then(setWhy, () => setWhy(null));
  }, [tx]);

  const decide = async (status: Status) => {
    setSaving(true);
    try {
      setInv(await api.decide(tx, status, note));
      onDecided();
    } catch (e) {
      setError(String(e));
    } finally {
      setSaving(false);
    }
  };

  if (error && !txn) return <div className="card error">{error}</div>;
  if (!txn) return <div className="card muted">Loading {tx}…</div>;
  const risk = inv?.risk;
  const components = risk?.components ?? {
    ...(txn.ml_score != null && { ml: txn.ml_score }),
    ...(txn.anomaly_score != null && { anomaly: txn.anomaly_score }),
    ...(txn.graph_score != null && { graph: txn.graph_score }),
  };
  const evidence = inv?.evidence ?? {};
  const graphFacts = Object.entries(GRAPH_LABELS).filter(([k]) => k in evidence);

  return (
    <div className="card">
      <div className="row" style={{ justifyContent: "space-between" }}>
        <h2 style={{ margin: 0 }} className="mono">{tx}</h2>
        {inv && <span className="secondary">{statusLabel(inv.status)}</span>}
      </div>

      <Section title="Transaction">
        <Fact label="Time (UTC)">{fmt.time(txn.event_time)}</Fact>
        <Fact label="Customer">
          <a href={`#/graph/${txn.customer_id}`} className="mono">{txn.customer_id}</a>
        </Fact>
        <Fact label="Type · amount">{txn.transaction_type} · {fmt.money(txn.amount, txn.currency)}</Fact>
        <Fact label="From → to"><span className="mono">{txn.source_account} → {txn.destination_account}</span></Fact>
        <Fact label="Device · IP"><span className="mono">{txn.device_id ?? "–"} · {txn.ip_address ?? "–"}</span></Fact>
        <Fact label="Location">{[txn.city, txn.country].filter(Boolean).join(", ") || "–"}</Fact>
      </Section>

      <Section title="Risk score">
        <div className="row" style={{ gap: 12 }}>
          <span style={{ fontSize: 28, fontWeight: 600 }}>{risk?.score ?? txn.risk_score ?? "–"}</span>
          <RiskBadge level={risk?.level ?? txn.risk_level} />
          <span className="secondary">{(risk?.rule_hits ?? txn.rule_hits ?? []).map((r) => `rule: ${r}`).join(" · ")}</span>
        </div>
        {risk?.missing_signals.length ? (
          <div className="muted">Scored without: {risk.missing_signals.join(", ")}</div>
        ) : null}
      </Section>

      <Section title="Signals (calibrated fraud probabilities)">
        <div className="grid" style={{ gap: 10 }}>
          <Meter label="ML prediction" value={components.ml ?? null} note="LightGBM on transaction features" />
          <Meter label="Anomaly score" value={components.anomaly ?? null} note="Isolation Forest on legitimate traffic" />
          <Meter label="Graph risk" value={components.graph ?? null} note="LightGBM on graph features" />
        </div>
      </Section>

      <Section title="Graph relationships">
        {graphFacts.length ? (
          graphFacts.map(([k, label]) => (
            <Fact key={k} label={label}>
              {k === "graph.is_new_payee" ? (evidence[k] ? "yes" : "no") : fmt.int(evidence[k] as number)}
            </Fact>
          ))
        ) : (
          <div className="muted">No graph evidence stored for this transaction.</div>
        )}
        <a href={`#/graph/${txn.customer_id}`}>Open the customer's graph →</a>
      </Section>

      <Section title="AI explanation">
        {why ? (
          <>
            <p style={{ margin: "0 0 8px" }}>{why.summary}</p>
            <ol style={{ margin: 0, paddingLeft: 20 }}>
              {why.reasons.map((r, i) => (
                <li key={i} style={{ marginBottom: 6 }}>
                  {r.text}
                  <div className="muted mono" style={{ fontSize: 11 }}>
                    {Object.entries(r.evidence).map(([k, v]) => `${k}=${typeof v === "number" ? +v.toFixed(4) : String(v)}`).join(" · ")}
                  </div>
                </li>
              ))}
            </ol>
            <div className="muted" style={{ fontSize: 12, marginTop: 6 }}>
              Generator: {why.generator}. Every reason cites the evidence above; uncited reasons are dropped.
            </div>
          </>
        ) : (
          <div className="muted">
            {inv === null ? "Below the alert threshold: no investigation, no explanation." : "Loading…"}
          </div>
        )}
      </Section>

      {inv && (
        <Section title="Decision">
          <div className="grid" style={{ gap: 8 }}>
            <input placeholder="Note (optional)" value={note} onChange={(e) => setNote(e.target.value)} />
            <div className="row">
              <button className="primary" disabled={saving} onClick={() => decide("CONFIRMED_FRAUD")}>Confirm fraud</button>
              <button disabled={saving} onClick={() => decide("FALSE_POSITIVE")}>False positive</button>
              <button disabled={saving} onClick={() => decide("DISMISSED")}>Dismiss</button>
              {inv.status !== "OPEN" && <button disabled={saving} onClick={() => decide("OPEN")}>Reopen</button>}
            </div>
            {error && <div className="error">{error}</div>}
          </div>
        </Section>
      )}
      <div style={{ marginTop: 12 }}>
        <a onClick={() => go("#/investigations/OPEN")} href="#/investigations/OPEN">← Back to the queue</a>
      </div>
    </div>
  );
}

export function Investigations({ status, tx, go }: { status: Status; tx?: string; go: (h: string) => void }) {
  const [cases, setCases] = useState<Investigation[] | null>(null);
  const [version, setVersion] = useState(0);

  useEffect(() => {
    setCases(null);
    api.investigations(status).then(setCases, () => setCases([]));
  }, [status, version]);

  return (
    <div className="grid split">
      <div className="card">
        <div className="row" style={{ justifyContent: "space-between", marginBottom: 12 }}>
          <h2 style={{ margin: 0 }}>Investigations</h2>
          <select value={status} onChange={(e) => go(`#/investigations/${e.target.value}`)}>
            {STATUSES.map((s) => (
              <option key={s} value={s}>{statusLabel(s)}</option>
            ))}
          </select>
        </div>
        {cases == null ? (
          <div className="muted">Loading…</div>
        ) : cases.length === 0 ? (
          <div className="muted">No {statusLabel(status).toLowerCase()} investigations.</div>
        ) : (
          <div className="scroll" style={{ maxHeight: 720, overflowY: "auto" }}>
            <table className="data">
              <tbody>
                {cases.map((c) => (
                  <tr
                    key={c.transaction_id}
                    className={`clickable${c.transaction_id === tx ? " selected" : ""}`}
                    onClick={() => go(`#/case/${c.transaction_id}`)}
                  >
                    <td>
                      <div className="mono">{c.transaction_id}</div>
                      <div className="muted" style={{ fontSize: 12 }}>{c.customer_id} · {fmt.time(c.event_time)}</div>
                    </td>
                    <td><RiskBadge level={c.risk.level} score={c.risk.score} /></td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
      {tx ? (
        <Case tx={tx} go={go} onDecided={() => setVersion((v) => v + 1)} />
      ) : (
        <div className="card muted">Select an investigation to see its evidence and explanation.</div>
      )}
    </div>
  );
}
