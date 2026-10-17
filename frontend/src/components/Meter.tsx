/** A probability in [0, 1] as a meter: blue fill on a lighter step of the same ramp. */
export function Meter({ label, value, note }: { label: string; value: number | null; note?: string }) {
  return (
    <div style={{ display: "grid", gridTemplateColumns: "120px 1fr 48px", gap: 10, alignItems: "center" }}>
      <span className="secondary">{label}</span>
      {value == null ? (
        <span className="muted">missing</span>
      ) : (
        <div
          role="meter"
          aria-valuemin={0}
          aria-valuemax={1}
          aria-valuenow={value}
          aria-label={label}
          style={{ height: 8, borderRadius: 4, background: "var(--meter-track)", overflow: "hidden" }}
        >
          <div style={{ width: `${Math.max(value * 100, 1)}%`, height: "100%", borderRadius: 4, background: "var(--accent)" }} />
        </div>
      )}
      <span style={{ textAlign: "right", fontVariantNumeric: "tabular-nums" }}>
        {value == null ? "" : `${(value * 100).toFixed(0)}%`}
      </span>
      {note && (
        <span className="muted" style={{ gridColumn: "2 / 4", fontSize: 12, marginTop: -6 }}>
          {note}
        </span>
      )}
    </div>
  );
}
