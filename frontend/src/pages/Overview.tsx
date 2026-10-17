import { useEffect, useState, type ReactNode } from "react";
import { api, fmt, type Level, type Overview as OverviewData, type RiskEvent } from "../api";
import { LevelIcon, RiskBadge, levelLabel } from "../components/RiskBadge";
import { Sparkline } from "../components/Sparkline";

const LEVELS: Level[] = ["HIGH", "MEDIUM", "LOW"];
const today = () => new Date().toISOString().slice(0, 10);

function Tile({ label, value, children }: { label: string; value: string; children?: ReactNode }) {
  return (
    <div className="card">
      <div className="secondary" style={{ fontSize: 12 }}>{label}</div>
      <div style={{ fontSize: 26, fontWeight: 600, margin: "4px 0 2px" }}>{value}</div>
      {children}
    </div>
  );
}

export function Overview({ go }: { go: (hash: string) => void }) {
  const [data, setData] = useState<OverviewData | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [day, setDay] = useState(today());
  const [feed, setFeed] = useState<RiskEvent[] | null>(null);

  useEffect(() => {
    let alive = true;
    const load = () =>
      api.overview().then(
        (d) => alive && (setData(d), setError(null)),
        (e: Error) => alive && setError(e.message),
      );
    load();
    const timer = setInterval(load, 10_000);
    return () => {
      alive = false;
      clearInterval(timer);
    };
  }, []);

  useEffect(() => {
    setFeed(null);
    api.alerts(day).then((r) => setFeed(r.events), () => setFeed([]));
  }, [day]);

  const total = data?.scored ?? 0;
  const decided = (data?.investigations.CONFIRMED_FRAUD ?? 0) + (data?.investigations.FALSE_POSITIVE ?? 0);

  return (
    <div className="grid" style={{ gap: 16 }}>
      {error && <div className="error">Overview unavailable: {error}</div>}
      {data && !data.metrics_available && (
        <div className="muted">Pipeline metrics (Prometheus) are unavailable; analyst figures are still live.</div>
      )}
      <div className="grid tiles">
        <Tile label="Transactions scored (24 h)" value={fmt.compact(data?.scored)} />
        <Tile label="Transactions per second" value={data?.transactions_per_s == null ? "–" : data.transactions_per_s.toFixed(1)}>
          <Sparkline points={data?.throughput ?? []} unit="tx/s" height={40} />
        </Tile>
        <Tile label="High-risk transactions (24 h)" value={fmt.int(data?.by_level.HIGH)}>
          <div className="muted" style={{ fontSize: 12 }}>each one opens an investigation</div>
        </Tile>
        <Tile label="Fraud detected (analyst-confirmed)" value={fmt.int(data?.investigations.CONFIRMED_FRAUD)}>
          <div className="muted" style={{ fontSize: 12 }}>{fmt.int(data?.investigations.OPEN)} cases open</div>
        </Tile>
        <Tile label="Average risk score (24 h)" value={data?.avg_risk_score == null ? "–" : data.avg_risk_score.toFixed(1)}>
          <div className="muted" style={{ fontSize: 12 }}>on a 0–100 scale</div>
        </Tile>
        <Tile label="Model precision (analyst-confirmed)" value={fmt.pct(data?.confirmed_precision)}>
          <div className="muted" style={{ fontSize: 12 }}>
            {decided ? `${decided} decided cases` : "no decided cases yet"}
          </div>
        </Tile>
      </div>

      <div className="grid two">
        <div className="card">
          <h2>Risk levels (24 h)</h2>
          <table className="data">
            <tbody>
              {LEVELS.map((level) => {
                const n = data?.by_level[level] ?? 0;
                return (
                  <tr key={level}>
                    <td style={{ width: 120 }}>
                      <span className="row" style={{ gap: 6 }}>
                        <LevelIcon level={level} /> {levelLabel(level)}
                      </span>
                    </td>
                    <td className="num">{fmt.int(n)}</td>
                    <td className="num secondary" style={{ width: 80 }}>
                      {total ? fmt.pct(n / total, n / total < 0.01 ? 2 : 0) : "–"}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
        <div className="card">
          <h2>Investigations</h2>
          <table className="data">
            <tbody>
              {(["OPEN", "CONFIRMED_FRAUD", "FALSE_POSITIVE", "DISMISSED"] as const).map((s) => (
                <tr key={s} className="clickable" onClick={() => go(`#/investigations/${s}`)}>
                  <td>{s.replace("_", " ").toLowerCase().replace(/^\w/, (c) => c.toUpperCase())}</td>
                  <td className="num">{fmt.int(data?.investigations[s])}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      <div className="card">
        <div className="row" style={{ justifyContent: "space-between", marginBottom: 12 }}>
          <h2 style={{ margin: 0 }}>Alert feed</h2>
          <label className="row secondary">
            Day (UTC) <input type="date" value={day} onChange={(e) => setDay(e.target.value)} />
          </label>
        </div>
        <div className="sub" style={{ margin: "0 0 12px" }}>
          Medium- and high-risk transactions of the day, newest first (Cassandra, 8 bucket partitions merged).
        </div>
        {feed == null ? (
          <div className="muted">Loading…</div>
        ) : feed.length === 0 ? (
          <div className="muted">No risky transactions on {day}.</div>
        ) : (
          <div className="scroll">
            <table className="data">
              <thead>
                <tr>
                  <th>Time (UTC)</th><th>Transaction</th><th>Customer</th><th>Type</th>
                  <th className="num">Amount</th><th>Risk</th><th>Rules</th>
                </tr>
              </thead>
              <tbody>
                {feed.map((e) => (
                  <tr key={e.transaction_id} className="clickable" onClick={() => go(`#/case/${e.transaction_id}`)}>
                    <td>{fmt.time(e.event_time)}</td>
                    <td className="mono">{e.transaction_id}</td>
                    <td className="mono">{e.customer_id}</td>
                    <td>{e.transaction_type}</td>
                    <td className="num">{fmt.int(e.amount)}</td>
                    <td><RiskBadge level={e.risk_level} score={e.risk_score} /></td>
                    <td className="secondary">{e.rule_hits?.join(", ") ?? ""}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}
