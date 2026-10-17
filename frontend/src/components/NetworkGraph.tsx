import { useState } from "react";
import type { Network } from "../api";
import { fmt } from "../lib/format";

// Radial layout around one customer: devices fan out on the left with the
// other customers seen on them; money counterparties sit on the right.
// Node kinds use three categorical slots (validated all-pairs, both themes),
// every node is labelled, and accounts an analyst confirmed as fraud carry a
// red ring that the legend explains: identity never rests on color alone.
const W = 820;
const H = 540;
const CX = W / 2;
const CY = H / 2;
const R_DEVICE = 140;
const R_SHARER = 232;
const R_PARTY = 205;
const MAX_SHARERS = 8;
export const MAX_PARTIES = 10;

type Kind = "customer" | "device" | "sharer" | "party";

interface Node {
  id: string;
  kind: Kind;
  x: number;
  y: number;
  angle?: number;
  detail: string;
  confirmed?: boolean;
}

interface Edge {
  from: Node;
  to: Node;
  arrow?: boolean;
}

const FILL: Record<Kind, string> = {
  customer: "var(--ink)",
  device: "var(--series-1)",
  sharer: "var(--series-2)",
  party: "var(--series-3)",
};
const RADIUS: Record<Kind, number> = { customer: 14, device: 8, sharer: 7, party: 8 };

const polar = (r: number, deg: number) => ({
  x: CX + r * Math.cos((deg * Math.PI) / 180),
  y: CY + r * Math.sin((deg * Math.PI) / 180),
});

const spread = (n: number, from: number, to: number) =>
  n === 1 ? [(from + to) / 2] : Array.from({ length: n }, (_, i) => from + ((to - from) * i) / (n - 1));

function layout(network: Network): { nodes: Node[]; edges: Edge[] } {
  const center: Node = { id: network.customer_id, kind: "customer", x: CX, y: CY, detail: "Customer under investigation" };
  const nodes: Node[] = [center];
  const edges: Edge[] = [];
  const sharers = new Map<string, Node>();

  spread(network.devices.length, 145, 215).forEach((angle, i) => {
    const d = network.devices[i];
    const node: Node = {
      id: d.device_id, kind: "device", ...polar(R_DEVICE, angle), angle,
      detail: `Device · used ${d.uses}× · shared with ${d.shared_with_count} other customer(s)`,
    };
    nodes.push(node);
    edges.push({ from: center, to: node });
    const shown = d.shared_with.slice(0, MAX_SHARERS);
    const width = Math.min(70, 9 * shown.length);
    spread(shown.length, angle - width / 2, angle + width / 2).forEach((a, j) => {
      const id = shown[j];
      let sharer = sharers.get(id);
      if (!sharer) {
        sharer = { id, kind: "sharer", ...polar(R_SHARER, a), angle: a, detail: "Another customer on this device · click to pivot" };
        sharers.set(id, sharer);
        nodes.push(sharer);
      }
      edges.push({ from: node, to: sharer });
    });
  });

  const parties = [...network.counterparties].sort((a, b) => b.total_amount - a.total_amount).slice(0, MAX_PARTIES);
  spread(parties.length, -60, 60).forEach((angle, i) => {
    const p = parties[i];
    const node: Node = {
      id: p.counterparty, kind: "party", ...polar(R_PARTY, angle), angle, confirmed: p.confirmed_fraud,
      detail: `${p.kind === "MERCHANT" ? "Merchant" : "Account"} · ${p.direction === "out" ? "paid by" : "paid"} this customer ${p.count}× · ${fmt.money(p.total_amount)}${p.confirmed_fraud ? " · confirmed fraud" : ""}`,
    };
    nodes.push(node);
    edges.push(p.direction === "out" ? { from: center, to: node, arrow: true } : { from: node, to: center, arrow: true });
  });
  return { nodes, edges };
}

function labelAt(n: Node) {
  const r = RADIUS[n.kind] + 6;
  if (n.angle == null) return { x: n.x, y: n.y + r + 9, anchor: "middle" as const };
  if (n.kind === "device") return { x: n.x, y: n.y - r - 2, anchor: "middle" as const };
  const cos = Math.cos((n.angle * Math.PI) / 180);
  const sin = Math.sin((n.angle * Math.PI) / 180);
  return { x: n.x + cos * r, y: n.y + sin * r + 4, anchor: cos >= 0 ? ("start" as const) : ("end" as const) };
}

const LEGEND: { kind: Kind; label: string }[] = [
  { kind: "customer", label: "Customer" },
  { kind: "device", label: "Device" },
  { kind: "sharer", label: "Other customer on a device" },
  { kind: "party", label: "Counterparty account" },
];

export function NetworkGraph({ network, onCustomer }: { network: Network; onCustomer: (id: string) => void }) {
  const [hover, setHover] = useState<Node | null>(null);
  const { nodes, edges } = layout(network);
  const linked = (e: Edge) => hover != null && (e.from === hover || e.to === hover);
  const dim = (n: Node) => hover != null && n !== hover && !edges.some((e) => linked(e) && (e.from === n || e.to === n));

  const trim = (e: Edge) => {
    const dx = e.to.x - e.from.x;
    const dy = e.to.y - e.from.y;
    const len = Math.hypot(dx, dy) || 1;
    // stop short of the target's ring (confirmed fraud) or halo (the customer)
    const r = RADIUS[e.to.kind] + (e.to.confirmed ? 7 : e.to.kind === "customer" ? 8 : 3);
    return { x2: e.to.x - (dx / len) * r, y2: e.to.y - (dy / len) * r };
  };

  return (
    <div>
      <div className="legend" style={{ marginBottom: 6 }}>
        {LEGEND.map((l) => (
          <span key={l.kind}>
            <svg width="12" height="12" aria-hidden="true"><circle cx="6" cy="6" r="5" fill={FILL[l.kind]} /></svg>
            {l.label}
          </span>
        ))}
        <span>
          <svg width="16" height="16" aria-hidden="true">
            <circle cx="8" cy="8" r="4" fill="var(--series-3)" />
            <circle cx="8" cy="8" r="7" fill="none" stroke="var(--critical)" strokeWidth="2" />
          </svg>
          Confirmed fraud (analyst)
        </span>
      </div>
      {/* below ~620px the labels would be unreadable: keep the size and pan instead */}
      <div style={{ overflowX: "auto" }}>
      <svg viewBox={`0 0 ${W} ${H}`} width="100%" style={{ display: "block", maxHeight: 580, minWidth: 620 }}
           role="img" aria-label={`Relationship graph of ${network.customer_id}`}>
        <defs>
          <marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
            <path d="M0,0 L10,5 L0,10 z" fill="var(--muted)" />
          </marker>
        </defs>
        {edges.map((e, i) => (
          <line key={i} x1={e.from.x} y1={e.from.y}
                {...(e.arrow ? trim(e) : { x2: e.to.x, y2: e.to.y })}
                stroke={linked(e) ? "var(--ink-2)" : "var(--axis)"}
                strokeOpacity={hover && !linked(e) ? 0.35 : 1}
                strokeWidth={e.from.kind === "device" ? 1 : 1.5}
                markerEnd={e.arrow ? "url(#arrow)" : undefined} />
        ))}
        {nodes.map((n) => {
          const clickable = n.kind === "sharer";
          const label = labelAt(n);
          return (
            <g key={`${n.kind}:${n.id}`} opacity={dim(n) ? 0.35 : 1}
               onMouseEnter={() => setHover(n)} onMouseLeave={() => setHover(null)}
               onClick={clickable ? () => onCustomer(n.id) : undefined}
               style={{ cursor: clickable ? "pointer" : "default" }}>
              <circle cx={n.x} cy={n.y} r={RADIUS[n.kind] + 7} fill="transparent" />
              {n.kind === "customer" && <circle cx={n.x} cy={n.y} r={RADIUS.customer + 6} fill="var(--accent-wash)" />}
              {n.confirmed && <circle cx={n.x} cy={n.y} r={RADIUS[n.kind] + 4} fill="none" stroke="var(--critical)" strokeWidth="2.5" />}
              <circle cx={n.x} cy={n.y} r={RADIUS[n.kind]} fill={FILL[n.kind]} stroke="var(--surface)" strokeWidth="2" />
              <text x={label.x} y={label.y} textAnchor={label.anchor}
                    fontSize={n.kind === "customer" ? 12.5 : 11} fontWeight={n.kind === "customer" ? 650 : 450}
                    fill={n.kind === "customer" ? "var(--ink)" : "var(--ink-2)"}>
                {n.id.length > 16 ? `${n.id.slice(0, 15)}…` : n.id}
              </text>
            </g>
          );
        })}
      </svg>
      </div>
      <div className="secondary" style={{ minHeight: 22, fontSize: 12.5 }}>
        {hover ? <><b style={{ color: "var(--ink)" }}>{hover.id}</b> · {hover.detail}</>
               : "Hover a node to see its links; click another customer to pivot the graph."}
      </div>
    </div>
  );
}
