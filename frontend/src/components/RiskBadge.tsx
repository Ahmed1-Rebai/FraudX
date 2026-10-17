import type { Level } from "../api";

// Risk levels are status, not series: status colors, always with icon + label.
const STATUS: Record<Level, { color: string; label: string }> = {
  LOW: { color: "var(--good)", label: "Low" },
  MEDIUM: { color: "var(--warning)", label: "Medium" },
  HIGH: { color: "var(--critical)", label: "High" },
};

export function LevelIcon({ level, size = 12 }: { level: Level; size?: number }) {
  const color = STATUS[level].color;
  return (
    <svg width={size} height={size} viewBox="0 0 12 12" aria-hidden="true" style={{ flex: "none" }}>
      {level === "LOW" && <circle cx="6" cy="6" r="5" fill={color} />}
      {level === "MEDIUM" && <path d="M6 1 L11 10.5 H1 Z" fill={color} />}
      {level === "HIGH" && (
        <path d="M4 1 H8 L11 4 V8 L8 11 H4 L1 8 V4 Z" fill={color} />
      )}
    </svg>
  );
}

export function RiskBadge({ level, score }: { level: Level | null; score?: number | null }) {
  if (!level) return <span className="muted">not scored</span>;
  return (
    <span style={{ display: "inline-flex", alignItems: "center", gap: 6 }}>
      <LevelIcon level={level} />
      <span>{STATUS[level].label}</span>
      {score != null && <span className="secondary">{score}</span>}
    </span>
  );
}

export const levelLabel = (level: Level) => STATUS[level].label;
