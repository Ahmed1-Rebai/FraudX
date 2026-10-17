import { useCallback, useEffect, useRef, useState, type RefObject } from "react";

export interface Polled<T> {
  data: T | null;
  error: string | null;
  loading: boolean;
  refresh: () => void;
}

/** Calls `load` now and every `intervalMs` (0 = once); re-runs when `deps` change. */
export function usePolling<T>(
  load: () => Promise<T>,
  intervalMs: number,
  deps: unknown[] = [],
): Polled<T> {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [tick, setTick] = useState(0);
  const loadRef = useRef(load);
  loadRef.current = load;

  useEffect(() => {
    let alive = true;
    setLoading(true);
    const run = () =>
      loadRef.current().then(
        (d) => {
          if (!alive) return;
          setData(d);
          setError(null);
          setLoading(false);
        },
        (e: Error) => {
          if (!alive) return;
          setError(e.message);
          setLoading(false);
        },
      );
    run();
    const timer = intervalMs ? setInterval(run, intervalMs) : undefined;
    return () => {
      alive = false;
      if (timer) clearInterval(timer);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [intervalMs, tick, ...deps]);

  const refresh = useCallback(() => setTick((t) => t + 1), []);
  return { data, error, loading, refresh };
}

/** Width of an element, kept up to date (charts draw to their container). */
export function useWidth(ref: RefObject<HTMLElement>): number {
  const [width, setWidth] = useState(0);
  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    setWidth(el.clientWidth);
    const observer = new ResizeObserver(([entry]) => setWidth(entry.contentRect.width));
    observer.observe(el);
    return () => observer.disconnect();
  }, [ref]);
  return width;
}
