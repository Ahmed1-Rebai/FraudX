import { useEffect, useRef, useState } from "react";
import { api, STATUSES, type Status } from "./api";
import { Sidebar, type Page, type Theme } from "./components/Shell";
import { ToastProvider } from "./components/Toast";
import { usePolling } from "./lib/hooks";
import { GraphPage } from "./pages/GraphPage";
import { Investigations } from "./pages/Investigations";
import { LiveStream } from "./pages/LiveStream";
import { Overview } from "./pages/Overview";

const THEME_KEY = "fraudx-theme";

function readTheme(): Theme {
  try {
    const t = localStorage.getItem(THEME_KEY);
    return t === "light" || t === "dark" ? t : "system";
  } catch {
    return "system";
  }
}

function useHash(): [string, (hash: string) => void] {
  const [hash, setHash] = useState(window.location.hash || "#/overview");
  useEffect(() => {
    const onChange = () => setHash(window.location.hash || "#/overview");
    window.addEventListener("hashchange", onChange);
    return () => window.removeEventListener("hashchange", onChange);
  }, []);
  return [hash, (h: string) => (window.location.hash = h)];
}

// Routes: #/overview, #/live, #/investigations/<STATUS>, #/case/<transaction>, #/graph/<customer>
export function App() {
  const [hash, go] = useHash();
  const [theme, setTheme] = useState<Theme>(readTheme);
  const counts = usePolling(api.counts, 15_000);
  const [route, rawArg] = hash.replace(/^#\//, "").split("/");
  const arg = rawArg ? decodeURIComponent(rawArg) : undefined;
  const page: Page = route === "case" ? "investigations" : (["live", "investigations", "graph"] as Page[]).includes(route as Page) ? (route as Page) : "overview";

  // The queue tab survives opening a case from it.
  const lastStatus = useRef<Status>("OPEN");
  if (route === "investigations") lastStatus.current = STATUSES.includes(arg as Status) ? (arg as Status) : "OPEN";

  useEffect(() => {
    if (theme === "system") document.documentElement.removeAttribute("data-theme");
    else document.documentElement.setAttribute("data-theme", theme);
    try {
      localStorage.setItem(THEME_KEY, theme);
    } catch {
      /* storage unavailable: the choice lasts for this visit */
    }
  }, [theme]);

  useEffect(() => window.scrollTo(0, 0), [page]);

  return (
    <ToastProvider>
      <div className="app">
        <Sidebar page={page} theme={theme} onTheme={setTheme} openCases={counts.data?.OPEN ?? null} />
        <main className="main">
          {page === "live" ? (
            <LiveStream go={go} />
          ) : page === "investigations" ? (
            <Investigations status={lastStatus.current} tx={route === "case" ? arg : undefined}
                            counts={counts.data} onChanged={counts.refresh} go={go} />
          ) : page === "graph" ? (
            <GraphPage customer={arg} go={go} />
          ) : (
            <Overview go={go} />
          )}
        </main>
      </div>
    </ToastProvider>
  );
}
