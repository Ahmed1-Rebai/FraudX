import { useState } from "react";

interface Props {
  points: { t: number; value: number }[];
  unit: string;
  height?: number;
}

/** One series over time: 2px line, 10% area wash, crosshair + tooltip on hover. */
export function Sparkline({ points, unit, height = 56 }: Props) {
  const [hover, setHover] = useState<number | null>(null);
  const width = 240;
  if (points.length < 2) return <div className="muted" style={{ height }}>No data yet</div>;

  const max = Math.max(...points.map((p) => p.value), 1e-9);
  const x = (i: number) => (i / (points.length - 1)) * width;
  const y = (v: number) => height - 4 - (v / max) * (height - 12);
  const line = points.map((p, i) => `${i ? "L" : "M"}${x(i)},${y(p.value)}`).join(" ");
  const area = `${line} L${width},${height} L0,${height} Z`;
  const active = hover == null ? null : points[hover];

  return (
    <div style={{ position: "relative" }}>
      <svg
        viewBox={`0 0 ${width} ${height}`}
        width="100%"
        height={height}
        preserveAspectRatio="none"
        role="img"
        aria-label={`Last 30 minutes, up to ${max.toFixed(1)} ${unit}`}
        onMouseLeave={() => setHover(null)}
        onMouseMove={(e) => {
          const rect = e.currentTarget.getBoundingClientRect();
          const i = Math.round(((e.clientX - rect.left) / rect.width) * (points.length - 1));
          setHover(Math.max(0, Math.min(points.length - 1, i)));
        }}
      >
        <line x1="0" x2={width} y1={height - 0.5} y2={height - 0.5} stroke="var(--axis)" strokeWidth="1" />
        <path d={area} fill="var(--accent-wash)" />
        <path d={line} fill="none" stroke="var(--accent)" strokeWidth="2" strokeLinejoin="round" strokeLinecap="round" vectorEffect="non-scaling-stroke" />
        {active && hover != null && (
          <line x1={x(hover)} x2={x(hover)} y1="0" y2={height} stroke="var(--muted)" strokeWidth="1" vectorEffect="non-scaling-stroke" />
        )}
      </svg>
      {active && hover != null && (
        <div
          style={{
            position: "absolute",
            top: -6,
            left: `${(hover / (points.length - 1)) * 100}%`,
            transform: `translate(${hover > points.length / 2 ? "-105%" : "5%"}, -100%)`,
            background: "var(--surface-raised)",
            border: "1px solid var(--border)",
            borderRadius: 6,
            padding: "4px 8px",
            fontSize: 12,
            whiteSpace: "nowrap",
            pointerEvents: "none",
          }}
        >
          <strong>{active.value.toFixed(1)}</strong> {unit}
          <span className="muted"> · {new Date(active.t * 1000).toLocaleTimeString("en-GB")}</span>
        </div>
      )}
      <div className="row muted" style={{ justifyContent: "space-between", fontSize: 11 }}>
        <span>30 min ago</span>
        <span>now</span>
      </div>
    </div>
  );
}
