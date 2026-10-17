import { useEffect, useState } from "react";
import { api, fmt, type Network, type Profile } from "../api";
import { MAX_PARTIES, NetworkGraph } from "../components/NetworkGraph";
import { RiskBadge } from "../components/RiskBadge";

/** Graph investigation: a customer's devices, the customers sharing them, and money flows (Neo4j). */
export function GraphPage({ customer, go }: { customer?: string; go: (h: string) => void }) {
  const [input, setInput] = useState(customer ?? "");
  const [network, setNetwork] = useState<Network | null>(null);
  const [profile, setProfile] = useState<Profile | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    setInput(customer ?? "");
    setNetwork(null);
    setProfile(null);
    setError(null);
    if (!customer) return;
    api.network(customer).then(setNetwork, (e: Error) => setError(e.message));
    api.profile(customer).then(setProfile, () => setProfile(null));
  }, [customer]);

  return (
    <div className="grid" style={{ gap: 16 }}>
      <form
        className="row"
        onSubmit={(e) => {
          e.preventDefault();
          if (input.trim()) go(`#/graph/${input.trim()}`);
        }}
      >
        <input placeholder="Customer id, e.g. C-000322" value={input} onChange={(e) => setInput(e.target.value)} />
        <button className="primary" type="submit">Show graph</button>
        {error && <span className="error">{error}</span>}
      </form>

      {customer && network && (
        <>
          <div className="grid tiles">
            <div className="card">
              <div className="secondary" style={{ fontSize: 12 }}>Latest risk</div>
              <div style={{ marginTop: 6 }}>
                {profile?.risk_profile ? (
                  <RiskBadge level={profile.risk_profile.level} score={profile.risk_profile.score} />
                ) : (
                  <span className="muted">not scored</span>
                )}
              </div>
            </div>
            <div className="card">
              <div className="secondary" style={{ fontSize: 12 }}>Home country · countries seen</div>
              <div style={{ marginTop: 6 }}>{profile?.country ?? "–"} · {profile?.countries.join(", ") || "–"}</div>
            </div>
            <div className="card">
              <div className="secondary" style={{ fontSize: 12 }}>Devices · IPs (recent)</div>
              <div style={{ marginTop: 6 }}>{profile?.devices.length ?? 0} · {profile?.known_ips.length ?? 0}</div>
            </div>
            <div className="card">
              <div className="secondary" style={{ fontSize: 12 }}>Active</div>
              <div style={{ marginTop: 6 }}>
                {profile?.first_seen && profile.last_seen
                  ? `${profile.first_seen.slice(0, 10)} → ${profile.last_seen.slice(0, 10)}`
                  : "–"}
              </div>
            </div>
          </div>

          <div className="card">
            <h2>Relationship graph of {customer}</h2>
            <div className="sub">
              Devices shared with other customers expose rings; counterparties show where money flows
              {network.counterparties.length > MAX_PARTIES &&
                ` (top ${MAX_PARTIES} of ${network.counterparties.length} by amount; all in the table below)`}
              .
            </div>
            {network.devices.length + network.counterparties.length === 0 ? (
              <div className="muted">No relationships stored yet.</div>
            ) : (
              <NetworkGraph network={network} onCustomer={(id) => go(`#/graph/${id}`)} />
            )}
          </div>

          <div className="grid two">
            <div className="card">
              <h2>Devices</h2>
              <div className="scroll">
                <table className="data">
                  <thead>
                    <tr><th>Device</th><th className="num">Uses</th><th className="num">Other customers</th><th>Last seen (UTC)</th></tr>
                  </thead>
                  <tbody>
                    {network.devices.map((d) => (
                      <tr key={d.device_id}>
                        <td className="mono">{d.device_id}</td>
                        <td className="num">{d.uses}</td>
                        <td className="num">{d.shared_with_count}</td>
                        <td>{fmt.time(d.last_seen)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
            <div className="card">
              <h2>Counterparties</h2>
              <div className="scroll">
                <table className="data">
                  <thead>
                    <tr><th>Account</th><th>Kind</th><th>Flow</th><th className="num">Count</th><th className="num">Total</th></tr>
                  </thead>
                  <tbody>
                    {network.counterparties.map((p) => (
                      <tr key={`${p.counterparty}-${p.direction}`}>
                        <td className="mono">{p.counterparty}</td>
                        <td>{p.kind?.toLowerCase() ?? "–"}</td>
                        <td>{p.direction === "out" ? "paid by customer" : "paid customer"}</td>
                        <td className="num">{p.count}</td>
                        <td className="num">{fmt.int(p.total_amount)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          </div>
        </>
      )}
      {!customer && <div className="card muted">Enter a customer id, or open one from an investigation.</div>}
    </div>
  );
}
