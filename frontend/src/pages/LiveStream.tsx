import { Activity, Pause, Play, Radio, Trash2 } from "lucide-react";
import { useEffect, useMemo, useRef, useState } from "react";
import { subscribeScored, type Level, type LiveEvent } from "../api";
import { LevelIcon, RiskBadge } from "../components/Risk";
import { Topbar } from "../components/Shell";
import { Card, CardHeader, Empty, Segmented } from "../components/ui";
import { fmt } from "../lib/format";

const KEEP = 250;
const RANK: Record<Level, number> = { LOW: 0, MEDIUM: 1, HIGH: 2 };

/** Scored transactions as they leave the scorer (SSE from transactions.scored). */
export function LiveStream({ go }: { go: (hash: string) => void }) {
  const [events, setEvents] = useState<LiveEvent[]>([]);
  const [fresh, setFresh] = useState<Set<string>>(new Set());
  const [connected, setConnected] = useState(false);
  const [paused, setPaused] = useState(false);
  const [minLevel, setMinLevel] = useState<Level>("LOW");
  const [query, setQuery] = useState("");
  const [rate, setRate] = useState(0);
  const [session, setSession] = useState({ total: 0, HIGH: 0, MEDIUM: 0 });
  const pausedRef = useRef(paused);
  const arrivals = useRef<number[]>([]);
  pausedRef.current = paused;

  useEffect(() => {
    let buffer: LiveEvent[] = [];
    const stop = subscribeScored((event) => {
      arrivals.current.push(Date.now());
      setSession((s) => ({
        total: s.total + 1,
        HIGH: s.HIGH + (event.level === "HIGH" ? 1 : 0),
        MEDIUM: s.MEDIUM + (event.level === "MEDIUM" ? 1 : 0),
      }));
      if (!pausedRef.current) buffer.push(event);
    }, setConnected);
    // Render in small frames so a burst of thousands of events stays smooth.
    const flush = setInterval(() => {
      if (!buffer.length) return;
      const batch = buffer.reverse();
      buffer = [];
      setEvents((prev) => [...batch, ...prev].slice(0, KEEP));
      setFresh(new Set(batch.map((e) => e.transaction_id)));
    }, 250);
    const meter = setInterval(() => {
      const since = Date.now() - 10_000;
      arrivals.current = arrivals.current.filter((t) => t >= since);
      setRate(arrivals.current.length / 10);
    }, 1000);
    return () => {
      stop();
      clearInterval(flush);
      clearInterval(meter);
    };
  }, []);

  const shown = useMemo(() => {
    const q = query.trim().toLowerCase();
    return events.filter(
      (e) =>
        RANK[e.level] >= RANK[minLevel] &&
        (!q || e.transaction_id.toLowerCase().includes(q) || e.customer_id.toLowerCase().includes(q)),
    );
  }, [events, minLevel, query]);

  return (
    <>
      <Topbar title="Live stream" subtitle="Every transaction as the scorer decides, in real time">
        <span className={`pill ${connected ? "good" : "warning"}`}>
          <span className={`live-dot${connected && !paused ? "" : " off"}`} />
          {connected ? (paused ? "Paused" : "Live") : "Connecting…"}
        </span>
      </Topbar>
      <div className="content">
        <div className="grid grid-kpi">
          <Card className="kpi">
            <div className="kpi-head"><span className="kpi-icon"><Activity size={16} /></span>Arrival rate</div>
            <div className="kpi-value num">{rate.toFixed(1)} <span className="muted" style={{ fontSize: 14 }}>tx/s</span></div>
            <div className="kpi-foot">last 10 seconds</div>
          </Card>
          <Card className="kpi">
            <div className="kpi-head"><span className="kpi-icon"><Radio size={16} /></span>Received this session</div>
            <div className="kpi-value num">{fmt.int(session.total)}</div>
            <div className="kpi-foot">since this page opened</div>
          </Card>
          <Card className="kpi">
            <div className="kpi-head"><span className="kpi-icon critical"><LevelIcon level="HIGH" size={14} /></span>High risk</div>
            <div className="kpi-value num">{fmt.int(session.HIGH)}</div>
            <div className="kpi-foot">{session.total ? fmt.pct(session.HIGH / session.total, 2) : "–"} of this session</div>
          </Card>
          <Card className="kpi">
            <div className="kpi-head"><span className="kpi-icon warning"><LevelIcon level="MEDIUM" size={14} /></span>Medium risk</div>
            <div className="kpi-value num">{fmt.int(session.MEDIUM)}</div>
            <div className="kpi-foot">sent to the review queue</div>
          </Card>
        </div>

        <Card>
          <CardHeader title="Scored transactions" hint={`Most recently scored first · last ${KEEP} kept`}>
            <div className="search">
              <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" aria-hidden>
                <circle cx="11" cy="11" r="7" /><path d="m20 20-3.5-3.5" />
              </svg>
              <input className="input" placeholder="Transaction or customer" value={query}
                     onChange={(e) => setQuery(e.target.value)} aria-label="Filter by transaction or customer" />
            </div>
            <Segmented label="Minimum level" value={minLevel} onChange={setMinLevel}
                       options={[{ value: "LOW", label: "All" }, { value: "MEDIUM", label: "Medium+" }, { value: "HIGH", label: "High" }]} />
            <button type="button" className="btn" onClick={() => setPaused((p) => !p)}>
              {paused ? <Play size={15} /> : <Pause size={15} />} {paused ? "Resume" : "Pause"}
            </button>
            <button type="button" className="btn ghost" onClick={() => setEvents([])} title="Clear the table">
              <Trash2 size={15} /> Clear
            </button>
          </CardHeader>
          <div className="card-body" style={{ paddingLeft: 6, paddingRight: 6 }}>
            {shown.length === 0 ? (
              <Empty title={events.length ? "No transaction matches the filters" : "Waiting for transactions…"} icon={Radio}>
                Inject one with <span className="mono">POST /transactions/simulate</span> or stream a dataset with{" "}
                <span className="mono">make replay ARGS="--rate 20 --rebase-time"</span>.
              </Empty>
            ) : (
              <div className="table-wrap" style={{ maxHeight: "calc(100vh - 340px)", minHeight: 320, overflowY: "auto" }}>
                <table className="table">
                  <thead><tr>
                    <th>Time (UTC)</th><th>Transaction · customer</th><th>Type</th>
                    <th className="right">Amount</th><th>Risk</th>
                    <th className="right" title="ML prediction">ML</th>
                    <th className="right" title="Anomaly score">Anom.</th>
                    <th className="right" title="Graph risk">Graph</th><th>Rules</th>
                  </tr></thead>
                  <tbody>
                    {shown.map((e) => (
                      <tr key={e.transaction_id}
                          className={`clickable ${e.level === "LOW" ? "" : e.level.toLowerCase()}${fresh.has(e.transaction_id) ? " fresh" : ""}`}
                          onClick={() => go(`#/case/${encodeURIComponent(e.transaction_id)}`)}>
                        <td className="num" title={fmt.dateTime(e.event_time)}>{fmt.time(e.event_time)}</td>
                        <td>
                          <div className="mono">{e.transaction_id}</div>
                          <div className="cell-sub">{e.customer_id}{e.country && ` · ${e.country}`}</div>
                        </td>
                        <td>{e.transaction_type}</td>
                        <td className="right num">{fmt.money(e.amount, e.currency)}</td>
                        <td><RiskBadge level={e.level} score={e.score} /></td>
                        <td className="right num">{fmt.pct(e.components.ml)}</td>
                        <td className="right num">{fmt.pct(e.components.anomaly)}</td>
                        <td className="right num">{fmt.pct(e.components.graph)}</td>
                        <td>{e.rule_hits.map((r) => <span key={r} className="chip rule">{r}</span>)}</td>
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
