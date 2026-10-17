import { useState } from "react";
import { fmt, type Network } from "../api";

// Radial layout around one customer: devices fan out on the left with the
// other customers seen on them; money counterparties sit on the right.
// Node kinds use three categorical slots (validated all-pairs, light and dark);
// every node is labelled, so identity never rests on color alone.
const W = 760;
const H = 520;
const CX = W / 2;
const CY = H / 2;
const R_DEVICE = 140;
const R_SHARER = 230;
const R_PARTY = 200;
const MAX_SHARERS = 8;
export const MAX_PARTIES = 10;

type Kind = "customer" | "device" | "sharer" | "party";

interface Node {
  id: string;
  kind: Kind;
  x: number;
  y: number;
  angle?: number; // degrees from the centre; labels sit outward along it
  detail: string;
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
const RADIUS: Record<Kind, number> = { customer: 13, device: 8, sharer: 7, party: 8 };

const polar = (r: number, deg: number) => ({
  x: CX + r * Math.cos((deg * Math.PI) / 180),
  y: CY + r * Math.sin((deg * Math.PI) / 180),
});

const spread = (n: number, from: number, to: number) =>
  n === 1 ? [(from + to) / 2] : Array.from({ length: n }, (_, i) => from + ((to - from) * i) / (n - 1));

function layout(network: Network): { nodes: Node[]; edges: Edge[] } {
  const center: Node = { id: network.customer_id, kind: "customer", x: CX, y: CY, detail: "customer under investigation" };
  const nodes: Node[] = [center];
  const edges: Edge[] = [];
  const sharers = new Map<string, Node>();

  const devices = network.devices;
  spread(devices.length, 145, 215).forEach((angle, i) => {
    const d = devices[i];
    const node: Node = {
      id: d.device_id, kind: "device", ...polar(R_DEVICE, angle), angle,
      detail: `device · used ${d.uses}× · shared with ${d.shared_with_count} other customer(s)`,
    };
    nodes.push(node);
    edges.push({ from: center, to: node });
    const shown = d.shared_with.slice(0, MAX_SHARERS);
    const width = Math.min(70, 9 * shown.length);
    spread(shown.length, angle - width / 2, angle + width / 2).forEach((a, j) => {
      const id = shown[j];
      let sharer = sharers.get(id);
      if (!sharer) {
        sharer = { id, kind: "sharer", ...polar(R_SHARER, a), angle: a, detail: "another customer on this device · click to investigate" };
        sharers.set(id, sharer);
        nodes.push(sharer);
      }
      edges.push({ from: node, to: sharer });
    });
  });

  const parties = [...network.counterparties]
    .sort((a, b) => b.total_amount - a.total_amount)
    .slice(0, MAX_PARTIES);
  spread(parties.length, -60, 60).forEach((angle, i) => {
    const p = parties[i];
    const node: Node = {
      id: p.counterparty, kind: "party", ...polar(R_PARTY, angle), angle,
      detail: `${p.kind?.toLowerCase() ?? "account"} · ${p.direction === "out" ? "paid by" : "paid"} this customer ${p.count}× · total ${fmt.int(p.total_amount)}`,
    };
    nodes.push(node);
    edges.push(p.direction === "out" ? { from: center, to: node, arrow: true } : { from: node, to: center, arrow: true });
  });
  return { nodes, edges };
}

/** Label position outward from the centre: right of right-side nodes, left of left-side ones. */
function labelAt(n: Node) {
  const r = RADIUS[n.kind] + 6;
  if (n.angle == null) return { x: n.x, y: n.y + r + 8, anchor: "middle" as const };
  const cos = Math.cos((n.angle * Math.PI) / 180);
  const sin = Math.sin((n.angle * Math.PI) / 180);
  if (n.kind === "device") return { x: n.x, y: n.y - r - 2, anchor: "middle" as const }; // above: edges run below
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

  const trim = (e: Edge) => {
    // stop the line at the target's ring so the arrowhead stays visible
    const dx = e.to.x - e.from.x;
    const dy = e.to.y - e.from.y;
    const len = Math.hypot(dx, dy) || 1;
    const r = RADIUS[e.to.kind] + 3;
    return { x2: e.to.x - (dx / len) * r, y2: e.to.y - (dy / len) * r };
  };

  return (
    <div>
      <div className="row" style={{ gap: 16, marginBottom: 8, fontSize: 12 }}>
        {LEGEND.map((l) => (
          <span key={l.kind} className="row" style={{ gap: 6 }}>
            <svg width="12" height="12" aria-hidden="true">
              <circle cx="6" cy="6" r="5" fill={FILL[l.kind]} />
            </svg>
            <span className="secondary">{l.label}</span>
          </span>
        ))}
      </div>
      <svg viewBox={`0 0 ${W} ${H}`} width="100%" role="img" aria-label={`Relationship graph of ${network.customer_id}`}>
        <defs>
          <marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
            <path d="M0,0 L10,5 L0,10 z" fill="var(--muted)" />
          </marker>
        </defs>
        {edges.map((e, i) => (
          <line
            key={i}
            x1={e.from.x}
            y1={e.from.y}
            {...(e.arrow ? trim(e) : { x2: e.to.x, y2: e.to.y })}
            stroke="var(--axis)"
            strokeWidth={e.from.kind === "device" ? 1 : 1.5}
            markerEnd={e.arrow ? "url(#arrow)" : undefined}
          />
        ))}
        {nodes.map((n) => {
          const clickable = n.kind === "sharer";
          return (
            <g
              key={`${n.kind}:${n.id}`}
              onMouseEnter={() => setHover(n)}
              onMouseLeave={() => setHover(null)}
              onClick={clickable ? () => onCustomer(n.id) : undefined}
              style={{ cursor: clickable ? "pointer" : "default" }}
            >
              <circle cx={n.x} cy={n.y} r={RADIUS[n.kind] + 6} fill="transparent" />
              <circle cx={n.x} cy={n.y} r={RADIUS[n.kind]} fill={FILL[n.kind]} stroke="var(--surface)" strokeWidth="2" />
              <text
                x={labelAt(n).x}
                y={labelAt(n).y}
                textAnchor={labelAt(n).anchor}
                fontSize={n.kind === "customer" ? 12 : 10}
                fontWeight={n.kind === "customer" ? 600 : 400}
                fill={n.kind === "customer" ? "var(--ink)" : "var(--ink-2)"}
              >
                {n.id.length > 16 ? `${n.id.slice(0, 15)}…` : n.id}
              </text>
            </g>
          );
        })}
      </svg>
      <div className="secondary" style={{ minHeight: 20, fontSize: 12 }}>
        {hover ? (
          <>
            <strong style={{ color: "var(--ink)" }}>{hover.id}</strong> · {hover.detail}
          </>
        ) : (
          "Hover a node for details; click another customer to pivot the graph."
        )}
      </div>
    </div>
  );
}
