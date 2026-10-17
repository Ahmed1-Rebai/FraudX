import { useEffect, useState } from "react";
import type { Status } from "./api";
import { GraphPage } from "./pages/GraphPage";
import { Investigations } from "./pages/Investigations";
import { LiveStream } from "./pages/LiveStream";
import { Overview } from "./pages/Overview";

type Theme = "system" | "light" | "dark";
const STATUSES: Status[] = ["OPEN", "CONFIRMED_FRAUD", "FALSE_POSITIVE", "DISMISSED"];

function readTheme(): Theme {
  try {
    const t = localStorage.getItem("fraudx-theme");
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

export function App() {
  const [hash, go] = useHash();
  const [theme, setTheme] = useState<Theme>(readTheme);
  const [page, arg] = hash.replace(/^#\//, "").split("/");

  useEffect(() => {
    if (theme === "system") document.documentElement.removeAttribute("data-theme");
    else document.documentElement.setAttribute("data-theme", theme);
    try {
      localStorage.setItem("fraudx-theme", theme);
    } catch {
      /* storage unavailable: the choice lasts for this visit */
    }
  }, [theme]);

  const nav = [
    ["overview", "Overview"],
    ["live", "Live stream"],
    ["investigations", "Investigations"],
    ["graph", "Graph"],
  ] as const;
  const current = page === "case" ? "investigations" : page;
  const status = STATUSES.includes(arg as Status) ? (arg as Status) : "OPEN";

  return (
    <div className="shell">
      <header className="top">
        <h1>FraudX</h1>
        <nav>
          {nav.map(([id, label]) => (
            <a key={id} href={`#/${id}`} className={current === id ? "active" : ""}>
              {label}
            </a>
          ))}
        </nav>
        <span className="spacer" />
        <select aria-label="Theme" value={theme} onChange={(e) => setTheme(e.target.value as Theme)}>
          <option value="system">System theme</option>
          <option value="light">Light</option>
          <option value="dark">Dark</option>
        </select>
      </header>
      {current === "live" ? (
        <LiveStream go={go} />
      ) : current === "investigations" ? (
        <Investigations status={status} tx={page === "case" ? arg && decodeURIComponent(arg) : undefined} go={go} />
      ) : current === "graph" ? (
        <GraphPage customer={arg ? decodeURIComponent(arg) : undefined} go={go} />
      ) : (
        <Overview go={go} />
      )}
    </div>
  );
}
