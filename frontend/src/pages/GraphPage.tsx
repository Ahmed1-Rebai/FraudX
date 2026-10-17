import { CalendarRange, Globe, Network as NetworkIcon, Search, ShieldAlert, Smartphone } from "lucide-react";
import { useEffect, useMemo, useState } from "react";
import { api, ApiError, type Network, type Profile } from "../api";
import { MAX_PARTIES, NetworkGraph } from "../components/NetworkGraph";
import { RiskBadge } from "../components/Risk";
import { Topbar } from "../components/Shell";
import { Card, CardHeader, Empty, ErrorBanner, Skeleton } from "../components/ui";
import { fmt, todayUtc } from "../lib/format";
import { usePolling } from "../lib/hooks";

/** Customers behind today's high-risk alerts: one click to their graph. */
function RecentCustomers() {
  const feed = usePolling(() => api.alerts(todayUtc(), "HIGH"), 60_000);
  const customers = useMemo(
    () => [...new Set((feed.data?.events ?? []).map((e) => e.customer_id))].slice(0, 10),
    [feed.data],
  );
  if (!customers.length) return null;
  return (
    <div className="row" style={{ gap: 8, flexWrap: "wrap", marginTop: 14 }}>
      <span className="muted" style={{ fontSize: 12.5 }}>Alerted today:</span>
      {customers.map((c) => (
        <a key={c} className="chip" href={`#/graph/${encodeURIComponent(c)}`}>{c}</a>
      ))}
    </div>
  );
}

/** Graph investigation: a customer's devices, the customers sharing them, and money flows (Neo4j). */
export function GraphPage({ customer, go }: { customer?: string; go: (hash: string) => void }) {
  const [input, setInput] = useState(customer ?? "");
  const [network, setNetwork] = useState<Network | null>(null);
  const [profile, setProfile] = useState<Profile | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let alive = true;
    setInput(customer ?? "");
    setNetwork(null);
    setProfile(null);
    setError(null);
    if (!customer) return;
    api.network(customer).then(
      (n) => alive && setNetwork(n),
      (e: Error) => alive && setError(e instanceof ApiError && e.status === 404 ? `No customer ${customer} in the graph.` : e.message),
    );
    api.profile(customer).then((p) => alive && setProfile(p), () => alive && setProfile(null));
    return () => {
      alive = false;
    };
  }, [customer]);

  const confirmed = network?.counterparties.filter((p) => p.confirmed_fraud).length ?? 0;
  const ring = network?.devices.reduce((n, d) => n + d.shared_with_count, 0) ?? 0;

  return (
    <>
      <Topbar title="Graph explorer" subtitle="Devices, shared-device rings and money flows around a customer (Neo4j)" />
      <div className="content">
        <Card>
          <div className="card-body" style={{ paddingTop: 18 }}>
            <form className="row" style={{ gap: 10, flexWrap: "wrap" }}
                  onSubmit={(e) => {
                    e.preventDefault();
                    if (input.trim()) go(`#/graph/${encodeURIComponent(input.trim())}`);
                  }}>
              <div className="search" style={{ flex: "1 1 260px", maxWidth: 420 }}>
                <Search size={15} aria-hidden />
                <input className="input" style={{ width: "100%" }} placeholder="Customer id, e.g. C-000322"
                       value={input} onChange={(e) => setInput(e.target.value)} aria-label="Customer id" />
              </div>
              <button className="btn primary" type="submit"><NetworkIcon size={15} /> Explore</button>
            </form>
            <RecentCustomers />
          </div>
        </Card>

        {error && <ErrorBanner>{error}</ErrorBanner>}

        {!customer && (
          <Card>
            <Empty title="Pick a customer" icon={NetworkIcon}>
              Enter a customer id, choose one alerted today, or open the graph from a case.
            </Empty>
          </Card>
        )}

        {customer && !error && (
          <>
            <div className="grid grid-kpi">
              <Card className="kpi">
                <div className="kpi-head"><span className="kpi-icon critical"><ShieldAlert size={16} /></span>Latest risk</div>
                <div style={{ minHeight: 30, display: "flex", alignItems: "center" }}>
                  {profile?.risk_profile ? <RiskBadge level={profile.risk_profile.level} score={profile.risk_profile.score} />
                    : <span className="muted">not scored yet</span>}
                </div>
                <div className="kpi-foot">
                  {profile?.risk_profile
                    ? <a href={`#/case/${encodeURIComponent(profile.risk_profile.transaction_id)}`}>{profile.risk_profile.transaction_id} →</a>
                    : "from the customer's last transaction"}
                </div>
              </Card>
              <Card className="kpi">
                <div className="kpi-head"><span className="kpi-icon"><Smartphone size={16} /></span>Devices</div>
                {network ? <div className="kpi-value num">{network.devices.length}</div> : <Skeleton height={30} width="50%" />}
                <div className="kpi-foot">{network ? `shared with ${ring} other customer${ring === 1 ? "" : "s"}` : " "}</div>
              </Card>
              <Card className="kpi">
                <div className="kpi-head"><span className="kpi-icon"><Globe size={16} /></span>Home country</div>
                <div className="kpi-value">{profile?.country ?? "–"}</div>
                <div className="kpi-foot">seen in {profile?.countries.join(", ") || "–"}</div>
              </Card>
              <Card className="kpi">
                <div className="kpi-head"><span className="kpi-icon"><CalendarRange size={16} /></span>Active</div>
                <div className="kpi-value" style={{ fontSize: 17 }}>
                  {profile?.first_seen && profile.last_seen ? `${fmt.date(profile.first_seen)} → ${fmt.date(profile.last_seen)}` : "–"}
                </div>
                <div className="kpi-foot">{profile ? `${profile.accounts.length} account(s) · ${profile.known_ips.length} recent IP(s)` : " "}</div>
              </Card>
            </div>

            <Card>
              <CardHeader title={`Relationship graph of ${customer}`}
                          hint={network && network.counterparties.length > MAX_PARTIES
                            ? `Top ${MAX_PARTIES} of ${network.counterparties.length} counterparties by amount; all are listed below`
                            : "Shared devices expose rings; arrows show where money flows"}>
                {confirmed > 0 && (
                  <span className="pill critical"><ShieldAlert size={13} aria-hidden /> {confirmed} confirmed-fraud counterpart{confirmed > 1 ? "ies" : "y"}</span>
                )}
              </CardHeader>
              <div className="card-body">
                {!network ? <Skeleton height={420} />
                  : network.devices.length + network.counterparties.length === 0
                    ? <Empty title="No relationships stored yet" icon={NetworkIcon} />
                    : <NetworkGraph network={network} onCustomer={(id) => go(`#/graph/${encodeURIComponent(id)}`)} />}
              </div>
            </Card>

            {network && (
              <div className="grid grid-2" style={{ alignItems: "start" }}>
                <Card>
                  <CardHeader title="Devices" hint="Most shared first" />
                  <div className="card-body" style={{ paddingLeft: 6, paddingRight: 6 }}>
                    {network.devices.length === 0 ? <Empty title="No devices" icon={Smartphone} /> : (
                      <div className="table-wrap">
                        <table className="table">
                          <thead><tr><th>Device</th><th className="right">Uses</th><th className="right">Other customers</th><th>Last seen (UTC)</th></tr></thead>
                          <tbody>
                            {network.devices.map((d) => (
                              <tr key={d.device_id}>
                                <td className="mono">{d.device_id}</td>
                                <td className="right num">{fmt.int(d.uses)}</td>
                                <td className="right num">{d.shared_with_count > 0 ? <b>{fmt.int(d.shared_with_count)}</b> : 0}</td>
                                <td className="num">{fmt.dateTime(d.last_seen)}</td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    )}
                  </div>
                </Card>
                <Card>
                  <CardHeader title="Counterparties" hint="Largest flows first" />
                  <div className="card-body" style={{ paddingLeft: 6, paddingRight: 6 }}>
                    {network.counterparties.length === 0 ? <Empty title="No money flows" /> : (
                      <div className="table-wrap">
                        <table className="table">
                          <thead><tr><th>Account</th><th>Flow</th><th className="right">Count</th><th className="right">Total</th></tr></thead>
                          <tbody>
                            {network.counterparties.map((p) => (
                              <tr key={`${p.account}-${p.counterparty}-${p.direction}`} className={p.confirmed_fraud ? "high" : ""}>
                                <td>
                                  <span className="mono">{p.counterparty}</span>
                                  {p.kind === "MERCHANT" && <span className="chip" style={{ marginLeft: 6 }}>merchant</span>}
                                  {p.confirmed_fraud && <span className="pill critical" style={{ marginLeft: 6 }}><ShieldAlert size={12} aria-hidden /> confirmed fraud</span>}
                                </td>
                                <td>{p.direction === "out" ? "paid by customer" : "paid customer"}</td>
                                <td className="right num">{fmt.int(p.count)}</td>
                                <td className="right num">{fmt.money(p.total_amount)}</td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    )}
                  </div>
                </Card>
              </div>
            )}
          </>
        )}
      </div>
    </>
  );
}
