import {
  Activity,
  BookOpen,
  ChartLine,
  LayoutDashboard,
  Monitor,
  Moon,
  Network,
  Server,
  ShieldAlert,
  ShieldCheck,
  Sun,
  type LucideIcon,
} from "lucide-react";
import type { ReactNode } from "react";
import { api } from "../api";
import { usePolling } from "../lib/hooks";

export type Page = "overview" | "live" | "investigations" | "graph";
export type Theme = "system" | "light" | "dark";

const NAV: { id: Page; label: string; icon: LucideIcon }[] = [
  { id: "overview", label: "Overview", icon: LayoutDashboard },
  { id: "live", label: "Live stream", icon: Activity },
  { id: "investigations", label: "Investigations", icon: ShieldAlert },
  { id: "graph", label: "Graph explorer", icon: Network },
];

const GRAFANA = import.meta.env.VITE_GRAFANA_URL || "http://localhost:3000/d/fraudx-pipeline";

export function Sidebar({
  page,
  theme,
  onTheme,
  openCases,
}: {
  page: Page;
  theme: Theme;
  onTheme: (t: Theme) => void;
  openCases: number | null;
}) {
  return (
    <aside className="sidebar">
      <div className="brand">
        <span className="brand-mark">
          <ShieldCheck size={18} aria-hidden />
        </span>
        <div>
          <div className="brand-name">FraudX</div>
          <div className="brand-tag">Real-time fraud intelligence</div>
        </div>
      </div>
      <div className="nav-label">Monitor</div>
      {NAV.map(({ id, label, icon: Icon }) => (
        <a key={id} href={`#/${id}`} className={`nav-item${page === id ? " active" : ""}`} aria-label={label}
           aria-current={page === id ? "page" : undefined}>
          <Icon size={17} aria-hidden />
          <span className="nav-text">{label}</span>
          {id === "live" && <span className="live-dot" aria-hidden />}
          {id === "investigations" && openCases != null && openCases > 0 && (
            <span className="count" aria-label={`${openCases} open`}>
              {openCases > 999 ? "999+" : openCases}
            </span>
          )}
        </a>
      ))}
      <div className="sidebar-footer">
        <a className="nav-item" href={GRAFANA} target="_blank" rel="noreferrer">
          <ChartLine size={16} aria-hidden /> Grafana
        </a>
        <a className="nav-item" href="/api/docs" target="_blank" rel="noreferrer">
          <BookOpen size={16} aria-hidden /> API reference
        </a>
        <div className="theme-switch" role="group" aria-label="Theme">
          {(
            [
              ["light", Sun, "Light"],
              ["system", Monitor, "System"],
              ["dark", Moon, "Dark"],
            ] as const
          ).map(([value, Icon, label]) => (
            <button key={value} type="button" aria-pressed={theme === value} title={label}
                    onClick={() => onTheme(value)}>
              <Icon size={15} aria-hidden />
              <span className="sr-only">{label}</span>
            </button>
          ))}
        </div>
      </div>
    </aside>
  );
}

/** Readiness of the five backends, from GET /health/ready. */
function HealthPill() {
  const { data, error } = usePolling(api.readiness, 15_000);
  const deps = data ? Object.entries(data.dependencies) : [];
  const down = deps.filter(([, d]) => !d.ok).map(([name]) => name);
  const tone = error ? "critical" : !data ? "" : down.length ? "warning" : "good";
  const text = error
    ? "API unreachable"
    : !data
      ? "Checking systems…"
      : down.length
        ? `Degraded: ${down.join(", ")}`
        : "All systems operational";
  return (
    <div className="popover-host">
      <button type="button" className={`pill ${tone}`} style={{ cursor: "default" }}>
        <Server size={14} aria-hidden /> {text}
      </button>
      {data && (
        <div className="popover" role="tooltip">
          {deps.map(([name, d]) => (
            <div key={name} className="dep">
              <span className="dot" style={{ background: d.ok ? "var(--good)" : "var(--critical)" }} />
              <span>
                <b style={{ fontWeight: 600 }}>{name}</b>
                <div className="muted" style={{ fontSize: 12 }}>{d.detail}</div>
              </span>
              <span className="muted num">{d.latency_ms.toFixed(0)} ms</span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

export function Topbar({ title, subtitle, children }: { title: string; subtitle: string; children?: ReactNode }) {
  return (
    <header className="topbar">
      <div style={{ minWidth: 0 }}>
        <h1>{title}</h1>
        <p>{subtitle}</p>
      </div>
      <span className="spacer" />
      {children}
      <HealthPill />
    </header>
  );
}
