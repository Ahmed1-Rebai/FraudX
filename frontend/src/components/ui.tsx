import { CircleAlert, Inbox, type LucideIcon } from "lucide-react";
import type { ReactNode } from "react";

export function Card({ children, className = "" }: { children: ReactNode; className?: string }) {
  return <section className={`card ${className}`}>{children}</section>;
}

export function CardHeader({
  title,
  hint,
  icon: Icon,
  children,
}: {
  title: string;
  hint?: ReactNode;
  icon?: LucideIcon;
  children?: ReactNode;
}) {
  return (
    <div className="card-header">
      {Icon && <Icon size={16} className="muted" aria-hidden />}
      <div className="card-title">
        <h2>{title}</h2>
        {hint && <div className="hint">{hint}</div>}
      </div>
      <span className="spacer" />
      {children}
    </div>
  );
}

export function Skeleton({ height = 16, width = "100%" }: { height?: number; width?: number | string }) {
  return <div className="skeleton" style={{ height, width }} aria-hidden />;
}

export function SkeletonRows({ rows = 5 }: { rows?: number }) {
  return (
    <div style={{ display: "grid", gap: 12, padding: "6px 0" }} aria-busy="true">
      {Array.from({ length: rows }, (_, i) => (
        <Skeleton key={i} height={14} width={`${90 - (i % 3) * 12}%`} />
      ))}
    </div>
  );
}

export function Empty({
  title,
  children,
  icon: Icon = Inbox,
}: {
  title: string;
  children?: ReactNode;
  icon?: LucideIcon;
}) {
  return (
    <div className="empty">
      <Icon size={30} aria-hidden />
      <b>{title}</b>
      {children && <span>{children}</span>}
    </div>
  );
}

export function ErrorBanner({ children }: { children: ReactNode }) {
  return (
    <div className="error-banner" role="alert">
      <CircleAlert size={16} aria-hidden /> {children}
    </div>
  );
}

export function Segmented<T extends string>({
  value,
  options,
  onChange,
  label,
}: {
  value: T;
  options: { value: T; label: ReactNode; count?: number }[];
  onChange: (value: T) => void;
  label: string;
}) {
  return (
    <div className="segmented" role="group" aria-label={label}>
      {options.map((o) => (
        <button
          key={o.value}
          type="button"
          aria-pressed={o.value === value}
          onClick={() => onChange(o.value)}
        >
          {o.label}
          {o.count != null && <span className="n">{o.count.toLocaleString("en")}</span>}
        </button>
      ))}
    </div>
  );
}
