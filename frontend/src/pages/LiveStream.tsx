import { useEffect, useRef, useState } from "react";
import { fmt, subscribeScored, type Level, type LiveEvent } from "../api";
import { RiskBadge } from "../components/RiskBadge";

const KEEP = 200;
const RANK: Record<Level, number> = { LOW: 0, MEDIUM: 1, HIGH: 2 };

/** Scored transactions as they leave the scorer (SSE from transactions.scored). */
export function LiveStream({ go }: { go: (hash: string) => void }) {
  const [events, setEvents] = useState<LiveEvent[]>([]);
  const [connected, setConnected] = useState(false);
  const [paused, setPaused] = useState(false);
  const [minLevel, setMinLevel] = useState<Level>("LOW");
  const [rate, setRate] = useState(0);
  const pausedRef = useRef(paused);
  const arrivals = useRef<number[]>([]);
  pausedRef.current = paused;

  useEffect(() => {
    const stop = subscribeScored((event) => {
      arrivals.current.push(Date.now());
      if (!pausedRef.current) setEvents((prev) => [event, ...prev].slice(0, KEEP));
    }, setConnected);
    const timer = setInterval(() => {
      const since = Date.now() - 10_000;
      arrivals.current = arrivals.current.filter((t) => t >= since);
      setRate(arrivals.current.length / 10);
    }, 1000);
    return () => {
      stop();
      clearInterval(timer);
    };
  }, []);

  const shown = events.filter((e) => RANK[e.level] >= RANK[minLevel]);

  return (
    <div className="card">
      <div className="row" style={{ justifyContent: "space-between", marginBottom: 12 }}>
        <div className="row" style={{ gap: 12 }}>
          <h2 style={{ margin: 0 }}>Live transactions</h2>
          <span className="secondary">
            {connected ? `connected · ${rate.toFixed(1)} tx/s` : "connecting…"}
          </span>
        </div>
        <div className="row">
          <label className="row secondary">
            Show
            <select value={minLevel} onChange={(e) => setMinLevel(e.target.value as Level)}>
              <option value="LOW">all levels</option>
              <option value="MEDIUM">medium and high</option>
              <option value="HIGH">high only</option>
            </select>
          </label>
          <button onClick={() => setPaused((p) => !p)}>{paused ? "Resume" : "Pause"}</button>
          <button onClick={() => setEvents([])}>Clear</button>
        </div>
      </div>
      <div className="sub" style={{ margin: "0 0 12px" }}>
        The newest {KEEP} scored transactions. Inject one with <span className="mono">POST /transactions/simulate</span> or{" "}
        <span className="mono">make replay ARGS="--rate 50 --rebase-time"</span>.
      </div>
      {shown.length === 0 ? (
        <div className="muted">Waiting for transactions…</div>
      ) : (
        <div className="scroll">
          <table className="data">
            <thead>
              <tr>
                <th>Event time (UTC)</th><th>Transaction</th><th>Customer</th><th>Type</th>
                <th className="num">Amount</th><th>Country</th><th>Risk</th>
                <th className="num">ML</th><th className="num">Anomaly</th><th className="num">Graph</th><th>Rules</th>
              </tr>
            </thead>
            <tbody>
              {shown.map((e) => (
                <tr key={e.transaction_id} className="clickable" onClick={() => go(`#/case/${e.transaction_id}`)}>
                  <td>{fmt.time(e.event_time)}</td>
                  <td className="mono">{e.transaction_id}</td>
                  <td className="mono">{e.customer_id}</td>
                  <td>{e.transaction_type}</td>
                  <td className="num">{fmt.money(e.amount, e.currency)}</td>
                  <td>{e.country ?? ""}</td>
                  <td><RiskBadge level={e.level} score={e.score} /></td>
                  <td className="num">{fmt.pct(e.components.ml)}</td>
                  <td className="num">{fmt.pct(e.components.anomaly)}</td>
                  <td className="num">{fmt.pct(e.components.graph)}</td>
                  <td className="secondary">{e.rule_hits.join(", ")}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
