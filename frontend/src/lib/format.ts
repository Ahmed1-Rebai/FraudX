// Formatting helpers. Times are shown in UTC, like the pipeline's event times.

const compactFmt = new Intl.NumberFormat("en", { notation: "compact", maximumFractionDigits: 1 });
const intFmt = new Intl.NumberFormat("en", { maximumFractionDigits: 0 });

export const fmt = {
  int: (n: number | null | undefined) => (n == null ? "–" : intFmt.format(Math.round(n))),
  compact: (n: number | null | undefined) => (n == null ? "–" : compactFmt.format(n)),
  decimal: (n: number | null | undefined, digits = 1) => (n == null ? "–" : n.toFixed(digits)),
  money: (n: number, currency?: string) =>
    `${n.toLocaleString("en", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}${
      currency ? ` ${currency}` : ""
    }`,
  pct: (n: number | null | undefined, digits = 0) =>
    n == null ? "–" : `${(n * 100).toFixed(digits)}%`,
  dateTime: (iso: string) =>
    new Date(iso).toLocaleString("en-GB", {
      day: "2-digit",
      month: "short",
      year: "numeric",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      timeZone: "UTC",
    }),
  time: (iso: string | number) =>
    new Date(iso).toLocaleTimeString("en-GB", { timeZone: "UTC" }),
  clock: (epochSeconds: number) =>
    new Date(epochSeconds * 1000).toLocaleTimeString("en-GB", {
      hour: "2-digit",
      minute: "2-digit",
      timeZone: "UTC",
    }),
  date: (iso: string) =>
    new Date(iso).toLocaleDateString("en-GB", {
      day: "2-digit",
      month: "short",
      year: "numeric",
      timeZone: "UTC",
    }),
  /** "3 min ago" / "in 2 days" (replays rebased faster than real time land in the future). */
  relative: (iso: string) => {
    const seconds = (new Date(iso).getTime() - Date.now()) / 1000;
    const steps: [Intl.RelativeTimeFormatUnit, number][] = [
      ["second", 60],
      ["minute", 60],
      ["hour", 24],
      ["day", 30],
      ["month", 12],
      ["year", Infinity],
    ];
    let value = seconds;
    for (const [unit, size] of steps) {
      if (Math.abs(value) < size) {
        return new Intl.RelativeTimeFormat("en", { numeric: "auto" }).format(
          Math.round(value),
          unit,
        );
      }
      value /= size;
    }
    return "";
  },
  status: (s: string) => s.replace("_", " ").toLowerCase().replace(/^\w/, (c) => c.toUpperCase()),
  stage: (worker: string) =>
    worker.replace(/-/g, " ").replace(/^\w/, (c) => c.toUpperCase()),
};

/** Today's date in UTC, as YYYY-MM-DD. */
export const todayUtc = () => new Date().toISOString().slice(0, 10);
