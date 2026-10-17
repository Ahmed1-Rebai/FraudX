import type { Level, Status } from "../api";
import { fmt } from "../lib/format";

// Risk levels are status, not series: the fixed status palette, always with an
// icon and a label (never color alone).
export const LEVEL: Record<Level, { color: string; label: string; tone: string }> = {
  LOW: { color: "var(--good)", label: "Low", tone: "good" },
  MEDIUM: { color: "var(--warning)", label: "Medium", tone: "warning" },
  HIGH: { color: "var(--critical)", label: "High", tone: "critical" },
};

export function LevelIcon({ level, size = 12 }: { level: Level; size?: number }) {
  const color = LEVEL[level].color;
  return (
    <svg width={size} height={size} viewBox="0 0 12 12" aria-hidden="true" style={{ flex: "none" }}>
      {level === "LOW" && <circle cx="6" cy="6" r="5" fill={color} />}
      {level === "MEDIUM" && <path d="M6 1 L11 10.5 H1 Z" fill={color} />}
      {level === "HIGH" && <path d="M4 1 H8 L11 4 V8 L8 11 H4 L1 8 V4 Z" fill={color} />}
    </svg>
  );
}

export function RiskBadge({ level, score }: { level: Level | null; score?: number | null }) {
  if (!level) return <span className="muted">Not scored</span>;
  return (
    <span className="risk">
      <LevelIcon level={level} />
      {LEVEL[level].label}
      {score != null && <span className="score">{score}</span>}
    </span>
  );
}

const STATUS_TONE: Record<Status, string> = {
  OPEN: "accent",
  CONFIRMED_FRAUD: "critical",
  FALSE_POSITIVE: "good",
  DISMISSED: "",
};

export function StatusPill({ status }: { status: Status }) {
  return <span className={`pill ${STATUS_TONE[status]}`}>{fmt.status(status)}</span>;
}
