import { useMemo, useRef, useState } from "react";
import type { Level, Point } from "../api";
import { fmt } from "../lib/format";
import { useWidth } from "../lib/hooks";
import { LEVEL, LevelIcon } from "./Risk";

const M = { top: 12, right: 12, bottom: 26, left: 44 };

/** Round up to a clean axis maximum (1, 2, 2.5, 5 × 10^k) and return the ticks. */
function niceTicks(max: number, count = 4): number[] {
  if (max <= 0) return [0, 1];
  const raw = max / count;
  const mag = 10 ** Math.floor(Math.log10(raw));
  const step = [1, 2, 2.5, 5, 10].map((m) => m * mag).find((s) => s >= raw) ?? raw;
  const top = Math.ceil(max / step) * step;
  return Array.from({ length: Math.round(top / step) + 1 }, (_, i) => +(i * step).toFixed(6));
}

const axisLabel = (v: number) => (v >= 1000 ? fmt.compact(v) : String(+v.toFixed(2)));

/** Five time labels spread over the chart's window. */
function Axes({ ticks, y, width, plotH, t0, t1, x }: {
  ticks: number[]; y: (v: number) => number; width: number; plotH: number;
  t0: number; t1: number; x: (t: number) => number;
}) {
  const labels = Array.from({ length: 5 }, (_, i) => t0 + ((t1 - t0) * i) / 4);
  return (
    <g>
      {ticks.map((t) => (
        <g key={t}>
          <line x1={M.left} x2={width - M.right} y1={y(t)} y2={y(t)}
                stroke={t === 0 ? "var(--axis)" : "var(--grid)"} strokeWidth="1" />
          <text x={M.left - 8} y={y(t) + 4} textAnchor="end" fontSize="11" fill="var(--muted)">
            {axisLabel(t)}
          </text>
        </g>
      ))}
      {labels.map((t, i) => (
        <text key={i} x={x(t)} y={M.top + plotH + 18} fontSize="11" fill="var(--muted)"
              textAnchor={i === 0 ? "start" : i === labels.length - 1 ? "end" : "middle"}>
          {fmt.clock(t)}
        </text>
      ))}
    </g>
  );
}

/** The chart's time window: the last `windowS` seconds up to now (or the newest point). */
function timeWindow(points: { t: number }[], windowS: number): [number, number] {
  const t1 = Math.max(Date.now() / 1000, points.length ? points[points.length - 1].t : 0);
  return [t1 - windowS, t1];
}

/** Consecutive samples; a run ends where samples are missing, so no line bridges a gap. */
function runs(points: Point[]): Point[][] {
  const step = Math.min(...points.slice(1).map((p, i) => p.t - points[i].t));
  const out: Point[][] = [];
  points.forEach((p, i) => {
    if (i === 0 || p.t - points[i - 1].t > step * 1.5) out.push([]);
    out[out.length - 1].push(p);
  });
  return out;
}

/** One series over a fixed time window: 2px line, 10 % wash, crosshair + tooltip. */
export function AreaChart({ points, unit, windowS, height = 220 }: {
  points: Point[]; unit: string; windowS: number; height?: number;
}) {
  const ref = useRef<HTMLDivElement>(null);
  const width = useWidth(ref);
  const [hover, setHover] = useState<number | null>(null);
  const plotH = height - M.top - M.bottom;
  const ticks = useMemo(() => niceTicks(Math.max(0, ...points.map((p) => p.value))), [points]);
  const top = ticks[ticks.length - 1] || 1;
  const [t0, t1] = timeWindow(points, windowS);
  const x = (t: number) => M.left + ((t - t0) / windowS) * (width - M.left - M.right);
  const y = (v: number) => M.top + plotH - (v / top) * plotH;

  if (!points.length) {
    return <div ref={ref} className="empty" style={{ height }}>No traffic in this window yet</div>;
  }
  const active = hover == null ? null : points[hover];

  return (
    <div ref={ref} className="chart">
      {width > 0 && (
        <svg width={width} height={height} role="img" aria-label={`${unit} over time`}
             onMouseLeave={() => setHover(null)}
             onMouseMove={(e) => {
               const px = e.clientX - e.currentTarget.getBoundingClientRect().left;
               const th = t0 + ((px - M.left) / (width - M.left - M.right)) * windowS;
               let best = 0;
               points.forEach((p, i) => { if (Math.abs(p.t - th) < Math.abs(points[best].t - th)) best = i; });
               setHover(best);
             }}>
          <Axes ticks={ticks} y={y} width={width} plotH={plotH} t0={t0} t1={t1} x={x} />
          {runs(points).map((run) => {
            const line = run.map((p, i) => `${i ? "L" : "M"}${x(p.t).toFixed(1)},${y(p.value).toFixed(1)}`).join(" ");
            const first = x(run[0].t).toFixed(1);
            const last = x(run[run.length - 1].t).toFixed(1);
            return (
              <g key={run[0].t}>
                <path d={`${line} L${last},${y(0)} L${first},${y(0)} Z`} fill="var(--accent-wash)" />
                <path d={line} fill="none" stroke="var(--accent)" strokeWidth="2" strokeLinejoin="round" strokeLinecap="round" />
              </g>
            );
          })}
          {active && (
            <g>
              <line x1={x(active.t)} x2={x(active.t)} y1={M.top} y2={M.top + plotH} stroke="var(--axis)" strokeWidth="1" />
              <circle cx={x(active.t)} cy={y(active.value)} r="4.5" fill="var(--accent)" stroke="var(--surface)" strokeWidth="2" />
            </g>
          )}
        </svg>
      )}
      {active && (
        <div className="tooltip" style={{ left: Math.min(x(active.t) + 12, width - 150), top: 8 }}>
          <div className="t">{fmt.clock(active.t)} UTC</div>
          <b>{active.value.toFixed(1)}</b> {unit}
        </div>
      )}
    </div>
  );
}

/** Medium and high-risk counts per minute over a fixed window: stacked columns, HIGH at the base. */
export function LevelColumns({ series, windowS = 3600, height = 220 }: {
  series: { level: Level; points: Point[] }[]; windowS?: number; height?: number;
}) {
  const ref = useRef<HTMLDivElement>(null);
  const width = useWidth(ref);
  const [hover, setHover] = useState<number | null>(null);
  const columns = useMemo(() => {
    const byT = new Map<number, Record<Level, number>>();
    for (const s of series) {
      for (const p of s.points) {
        const row = byT.get(p.t) ?? { LOW: 0, MEDIUM: 0, HIGH: 0 };
        row[s.level] = p.value;
        byT.set(p.t, row);
      }
    }
    return [...byT.entries()].sort((a, b) => a[0] - b[0]).map(([t, v]) => ({ t, ...v }));
  }, [series]);
  const plotH = height - M.top - M.bottom;
  const plotW = width - M.left - M.right;
  const ticks = niceTicks(Math.max(0, ...columns.map((c) => c.HIGH + c.MEDIUM)), 3);
  const top = ticks[ticks.length - 1] || 1;
  const [t0, t1] = timeWindow(columns, windowS);
  const x = (t: number) => M.left + ((t - t0) / windowS) * plotW;
  // each point counts the minute that ends at its timestamp
  const barW = Math.max(2, Math.min(24, (60 / windowS) * plotW * 0.62));
  const left = (t: number) => x(t - 30) - barW / 2;
  const h = (v: number) => (v / top) * plotH;
  const base = M.top + plotH;

  if (!columns.some((c) => c.HIGH + c.MEDIUM > 0)) {
    return <div ref={ref} className="empty" style={{ height }}>No medium or high-risk transactions in the last hour</div>;
  }
  const active = hover == null ? null : columns[hover];
  // A rounded data-end on the top segment only; the base stays square.
  const topBar = (bx: number, by: number, bh: number) => {
    const r = Math.min(4, bh, barW / 2);
    return `M${bx},${by + bh} V${by + r} Q${bx},${by} ${bx + r},${by} H${bx + barW - r} Q${bx + barW},${by} ${bx + barW},${by + r} V${by + bh} Z`;
  };

  return (
    <div>
      <div className="legend" style={{ justifyContent: "flex-end", marginBottom: 6 }}>
        {(["HIGH", "MEDIUM"] as Level[]).map((l) => (
          <span key={l}><LevelIcon level={l} /> {LEVEL[l].label}</span>
        ))}
      </div>
      <div ref={ref} className="chart">
        {width > 0 && (
          <svg width={width} height={height} role="img" aria-label="Medium and high-risk transactions per minute"
               onMouseLeave={() => setHover(null)}
               onMouseMove={(e) => {
                 const px = e.clientX - e.currentTarget.getBoundingClientRect().left;
                 const th = t0 + ((px - M.left) / plotW) * windowS;
                 const i = columns.findIndex((c) => c.t - 60 <= th && th <= c.t && c.HIGH + c.MEDIUM > 0);
                 setHover(i < 0 ? null : i);
               }}>
            <Axes ticks={ticks} y={(v) => base - h(v)} width={width} plotH={plotH} t0={t0} t1={t1} x={x} />
            {columns.map((c, i) => {
              const highH = h(c.HIGH);
              const medH = h(c.MEDIUM);
              const gap = c.HIGH > 0 && c.MEDIUM > 0 ? 2 : 0;
              const bx = left(c.t);
              return (
                <g key={c.t} opacity={hover == null || hover === i ? 1 : 0.55}>
                  {c.HIGH > 0 && (c.MEDIUM > 0
                    ? <rect x={bx} y={base - highH} width={barW} height={highH} fill={LEVEL.HIGH.color} />
                    : <path d={topBar(bx, base - highH, highH)} fill={LEVEL.HIGH.color} />)}
                  {c.MEDIUM > 0 && (
                    <path d={topBar(bx, base - highH - gap - medH, Math.max(medH, 1))} fill={LEVEL.MEDIUM.color} />
                  )}
                </g>
              );
            })}
          </svg>
        )}
        {active && (
          <div className="tooltip" style={{ left: Math.min(left(active.t) + barW + 8, width - 150), top: 8 }}>
            <div className="t">{fmt.clock(active.t - 60)}–{fmt.clock(active.t)} UTC</div>
            <div className="row" style={{ gap: 6 }}><LevelIcon level="HIGH" /> High <b>{active.HIGH}</b></div>
            <div className="row" style={{ gap: 6 }}><LevelIcon level="MEDIUM" /> Medium <b>{active.MEDIUM}</b></div>
          </div>
        )}
      </div>
    </div>
  );
}

/** A small trend for a KPI tile (one series, no axes). */
export function Sparkline({ points, height = 34 }: { points: Point[]; height?: number }) {
  const ref = useRef<HTMLDivElement>(null);
  const width = useWidth(ref);
  if (points.length < 2) return <div ref={ref} style={{ height }} />;
  const max = Math.max(...points.map((p) => p.value), 1e-9);
  const x = (i: number) => (i / (points.length - 1)) * width;
  const y = (v: number) => height - 3 - (v / max) * (height - 8);
  const line = points.map((p, i) => `${i ? "L" : "M"}${x(i).toFixed(1)},${y(p.value).toFixed(1)}`).join(" ");
  return (
    <div ref={ref}>
      {width > 0 && (
        <svg width={width} height={height} aria-hidden="true">
          <path d={`${line} L${width},${height} L0,${height} Z`} fill="var(--accent-wash)" />
          <path d={line} fill="none" stroke="var(--accent)" strokeWidth="2" strokeLinejoin="round" />
        </svg>
      )}
    </div>
  );
}

/** The 0–100 risk score as a ring, colored by level. */
export function ScoreRing({ score, level, size = 116 }: { score: number; level: Level; size?: number }) {
  const r = size / 2 - 9;
  const c = 2 * Math.PI * r;
  return (
    <svg width={size} height={size} viewBox={`0 0 ${size} ${size}`} role="img" aria-label={`Risk score ${score} of 100`}>
      <circle cx={size / 2} cy={size / 2} r={r} fill="none" stroke="var(--track)" strokeWidth="9" />
      <circle cx={size / 2} cy={size / 2} r={r} fill="none" stroke={LEVEL[level].color} strokeWidth="9"
              strokeLinecap="round" strokeDasharray={`${(c * Math.max(score, 1)) / 100} ${c}`}
              transform={`rotate(-90 ${size / 2} ${size / 2})`} />
      <text x="50%" y="48%" textAnchor="middle" fontSize="30" fontWeight="700" fill="var(--ink)" letterSpacing="-1">{score}</text>
      <text x="50%" y="66%" textAnchor="middle" fontSize="11" fill="var(--muted)">of 100</text>
    </svg>
  );
}

/** A probability in [0, 1] as a meter (blue on a lighter track). */
export function Meter({ label, value, note }: { label: string; value: number | null; note?: string }) {
  return (
    <div className="signal">
      <span className="secondary">{label}</span>
      {value == null ? <span className="muted">missing</span> : <b className="num">{fmt.pct(value)}</b>}
      <div className="bar-track" role="meter" aria-valuemin={0} aria-valuemax={1} aria-valuenow={value ?? undefined} aria-label={label}>
        {value != null && <div className="bar-fill" style={{ width: `${Math.max(value * 100, 1.5)}%`, background: "var(--accent)" }} />}
      </div>
      {note && <span className="note">{note}</span>}
    </div>
  );
}
