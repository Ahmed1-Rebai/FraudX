import { Activity, BadgeCheck, Inbox, Siren, type LucideIcon } from "lucide-react";
import { useState, type ReactNode } from "react";
import { api, type Level, type Stage } from "../api";
import { AreaChart, LevelColumns, Sparkline } from "../components/charts";
import { LEVEL, LevelIcon, RiskBadge } from "../components/Risk";
import { Topbar } from "../components/Shell";
import { Card, CardHeader, Empty, ErrorBanner, Segmented, Skeleton, SkeletonRows } from "../components/ui";
import { fmt, todayUtc } from "../lib/format";
import { usePolling } from "../lib/hooks";

function Kpi({ icon: Icon, tone = "", label, value, foot, children }: {
  icon: LucideIcon; tone?: string; label: string; value: string | null; foot?: ReactNode; children?: ReactNode;
}) {
  return (
    <Card className="kpi">
      <div className="kpi-head">
        <span className={`kpi-icon ${tone}`}><Icon size={16} aria-hidden /></span>
        {label}
      </div>
      {value == null ? <Skeleton height={30} width="55%" /> : <div className="kpi-value num">{value}</div>}
      {children}
      {foot && <div className="kpi-foot">{foot}</div>}
    </Card>
  );
}

/** A stage is healthy while it keeps up: small backlog, sub-second latency. */
function stageTone(s: Stage): string {
  if (s.messages_per_s == null && s.lag == null) return "var(--muted)";
  if ((s.lag ?? 0) > 10_000 || (s.latency_p95_s ?? 0) > 30) return "var(--critical)";
  if ((s.lag ?? 0) > 1_000 || (s.latency_p95_s ?? 0) > 5) return "var(--warning)";
  return "var(--good)";
}

export function Overview({ go }: { go: (hash: string) => void }) {
  const overview = usePolling(api.overview, 10_000);
  const data = overview.data;
  const [day, setDay] = useState(todayUtc());
  const [feedLevel, setFeedLevel] = useState<"ALL" | Level>("ALL");
  const feed = usePolling(() => api.alerts(day, feedLevel === "ALL" ? undefined : feedLevel), 30_000, [day, feedLevel]);

  const total = data?.scored ?? 0;
  const alerts = data?.by_level.HIGH ?? null;
  const confirmed = data?.investigations.CONFIRMED_FRAUD ?? 0;
  const decided = confirmed + (data?.investigations.FALSE_POSITIVE ?? 0);
  const bands = data?.thresholds.MEDIUM != null && data.thresholds.HIGH != null
    ? ` · medium from ${data.thresholds.MEDIUM}, high from ${data.thresholds.HIGH}` : "";

  return (
    <>
      <Topbar title="Overview" subtitle="Real-time fraud monitoring · last 24 hours, UTC" />
      <div className="content">
        {overview.error && <ErrorBanner>Overview unavailable: {overview.error}</ErrorBanner>}
        {data && !data.metrics_available && (
          <ErrorBanner>Pipeline metrics (Prometheus) are unavailable; analyst figures are still live.</ErrorBanner>
        )}

        <div className="grid grid-kpi">
          <Kpi icon={Activity} label="Transactions scored" value={data ? fmt.compact(data.scored) : null}
               foot={data ? `${fmt.decimal(data.transactions_per_s)} tx/s now · last 24 hours` : undefined}>
            <Sparkline points={data?.throughput ?? []} />
          </Kpi>
          <Kpi icon={Siren} tone="critical" label="Alerts raised" value={data ? fmt.int(alerts) : null}
               foot={total ? `${fmt.pct((alerts ?? 0) / total, 2)} of scored traffic` : "–"} />
          <Kpi icon={Inbox} tone="warning" label="Open cases" value={data ? fmt.int(data.investigations.OPEN) : null}
               foot={<a href="#/investigations/OPEN">Open the analyst queue →</a>} />
          <Kpi icon={BadgeCheck} tone="good" label="Alert precision"
               value={data ? fmt.pct(data.confirmed_precision) : null}
               foot={decided ? `${confirmed} confirmed fraud of ${decided} decided cases` : "no decided cases yet"} />
        </div>

        <div className="grid grid-main">
          <Card>
            <CardHeader title="Throughput" hint="Transactions scored per second, last 30 minutes" />
            <div className="card-body">
              {data ? <AreaChart points={data.throughput} unit="tx/s" windowS={1800} /> : <Skeleton height={220} />}
            </div>
          </Card>
          <Card>
            <CardHeader title="Risk levels" hint={`Share of scored transactions, 24 h${bands}`} />
            <div className="card-body">
              {!data ? <SkeletonRows rows={3} /> : (
                <>
                  {(["HIGH", "MEDIUM", "LOW"] as Level[]).map((level) => {
                    const n = data.by_level[level] ?? 0;
                    const share = total ? n / total : 0;
                    return (
                      <div key={level} className="level-row">
                        <span className="row" style={{ gap: 8, fontWeight: 550 }}>
                          <LevelIcon level={level} /> {LEVEL[level].label}
                        </span>
                        <div className="bar-track">
                          <div className="bar-fill" style={{ width: `${Math.max(share * 100, n ? 1 : 0)}%`, background: LEVEL[level].color }} />
                        </div>
                        <span className="num" style={{ textAlign: "right", fontWeight: 600 }}>{fmt.int(n)}</span>
                        <span className="num muted" style={{ textAlign: "right" }}>{fmt.pct(share, share && share < 0.01 ? 2 : 0)}</span>
                      </div>
                    );
                  })}
                  <div className="stat-grid" style={{ marginTop: 14 }}>
                    <div className="stat"><div className="label">Average score</div>
                      <div className="value">{fmt.decimal(data.avg_risk_score)}<span className="muted" style={{ fontSize: 12, fontWeight: 500 }}> / 100</span></div></div>
                    <div className="stat"><div className="label">Alert threshold</div>
                      <div className="value">{data.alert_threshold == null ? "–" : `≥ ${data.alert_threshold}`}</div></div>
                  </div>
                </>
              )}
            </div>
          </Card>
        </div>

        <div className="grid grid-main">
          <Card>
            <CardHeader title="Medium and high-risk transactions" hint="Per minute, last 60 minutes" />
            <div className="card-body">
              {data ? <LevelColumns series={data.alerts_series} /> : <Skeleton height={240} />}
            </div>
          </Card>
          <Card>
            <CardHeader title="Pipeline" hint="Per worker: throughput, backlog, p95 latency" />
            <div className="card-body">
              {!data ? <SkeletonRows rows={6} /> : data.stages.map((s) => (
                <div key={s.worker} className="stage">
                  <span className="dot" style={{ background: stageTone(s) }} />
                  <span className="name">{fmt.stage(s.worker)}</span>
                  <span className="meta">
                    {s.messages_per_s == null ? "idle" : `${fmt.decimal(s.messages_per_s)} msg/s`}
                    {" · lag "}{fmt.int(s.lag)}
                    {s.latency_p95_s != null && ` · ${s.latency_p95_s < 1 ? `${Math.round(s.latency_p95_s * 1000)} ms` : `${s.latency_p95_s.toFixed(1)} s`}`}
                  </span>
                </div>
              ))}
            </div>
          </Card>
        </div>

        <Card>
          <CardHeader title="Alert feed" hint="Medium and high-risk transactions of the day, newest first (Cassandra)">
            <Segmented label="Risk level" value={feedLevel} onChange={setFeedLevel}
                       options={[{ value: "ALL", label: "All" }, { value: "HIGH", label: "High" }, { value: "MEDIUM", label: "Medium" }]} />
            <input className="input" type="date" value={day} aria-label="Day (UTC)" onChange={(e) => setDay(e.target.value)} />
          </CardHeader>
          <div className="card-body" style={{ paddingLeft: 6, paddingRight: 6 }}>
            {feed.loading && !feed.data ? <div style={{ padding: "0 14px" }}><SkeletonRows rows={6} /></div>
              : !feed.data?.events.length ? <Empty title={`No risky transactions on ${fmt.date(day)}`}>
                  Pick another day, or stream traffic with <span className="mono">make replay</span>.
                </Empty>
              : (
                <div className="table-wrap" style={{ maxHeight: 460, overflowY: "auto" }}>
                  <table className="table">
                    <thead><tr>
                      <th>Time (UTC)</th><th>Transaction</th><th>Customer</th><th>Type</th>
                      <th className="right">Amount</th><th>Risk</th><th>Rules</th>
                    </tr></thead>
                    <tbody>
                      {feed.data.events.map((e) => (
                        <tr key={e.transaction_id} className={`clickable ${e.risk_level.toLowerCase()}`}
                            onClick={() => go(`#/case/${encodeURIComponent(e.transaction_id)}`)}>
                          <td className="num">{fmt.time(e.event_time)}</td>
                          <td className="mono">{e.transaction_id}</td>
                          <td className="mono">{e.customer_id}</td>
                          <td>{e.transaction_type}</td>
                          <td className="right num">{fmt.money(e.amount)}</td>
                          <td><RiskBadge level={e.risk_level} score={e.risk_score} /></td>
                          <td>{(e.rule_hits ?? []).map((r) => <span key={r} className="chip rule">{r}</span>)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
          </div>
        </Card>
      </div>
    </>
  );
}
