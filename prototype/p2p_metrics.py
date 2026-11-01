#!/usr/bin/env python3
"""
Overlay metrics for the report. Asks every running lot server for its neighbor
list (request-reply op "peers"), rebuilds the overlay graph from those answers,
and prints the numbers Milestone 3 asks for:

    node degree, edge count, diameter, average path length
    lookup cost: hops and messages of real lookups (cache bypassed, only the
                 owner may answer, so hops = overlay distance to that lot)
    maintenance cost: ping traffic per round, gossip traffic per put
    gossip probability bound: P(no copy) = (1 - p_f)^n for each node's degree
    stretch: overlay hops / direct hops (direct is always 1 hop on one network)

    python3 p2p_metrics.py              # lots A..E on their default ports
    python3 p2p_metrics.py --lookups 0  # graph only, no lookup traffic
"""

import argparse
import itertools
import json
import sys
from collections import deque

from comm import request_reply
from comm.request_reply import ServiceUnavailable
from common import LOT_PORTS, P2P_GOSSIP_INTERVAL, P2P_MAINT_INTERVAL, MessageLog


def ask(port, op, args, log):
    try:
        r = request_reply.call("127.0.0.1", port, op, args, "metrics", log, timeout=3.0, retries=1)
    except ServiceUnavailable:
        return None
    return r.get("result") if r.get("status") == "OK" else None


def bfs(adj, start):
    dist = {start: 0}
    q = deque([start])
    while q:
        u = q.popleft()
        for v in adj.get(u, ()):
            if v not in dist:
                dist[v] = dist[u] + 1
                q.append(v)
    return dist


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--lookups", type=int, default=1, help="lookups per ordered pair of lots (0 = none)")
    ap.add_argument("--json", action="store_true")
    a = ap.parse_args()
    log = MessageLog("metrics")

    peers = {}
    for lot, port in sorted(LOT_PORTS.items()):
        r = ask(port, "peers", {}, log)
        if r:
            peers[lot] = r
    if len(peers) < 2:
        sys.exit("need at least two running lot servers (found %d)" % len(peers))

    adj = {lot: sorted(n["id"] for n in r["neighbors"]) for lot, r in peers.items()}
    edges = sorted({tuple(sorted((u, v))) for u in adj for v in adj[u]})
    n = len(adj)
    dists = {u: bfs(adj, u) for u in adj}
    reachable_pairs = [(u, v) for u in adj for v in adj if u < v and v in dists[u]]
    diameter = max((dists[u][v] for u, v in reachable_pairs), default=0)
    avg_path = (sum(dists[u][v] for u, v in reachable_pairs) / len(reachable_pairs)) if reachable_pairs else 0
    connected = all(len(dists[u]) == n for u in adj)
    p_f = next(iter(peers.values()))["gossip_prob"]

    out = {
        "nodes": n, "edges": len(edges), "edge_list": edges, "connected": connected,
        "degree": {u: len(adj[u]) for u in adj}, "avg_degree": round(2 * len(edges) / n, 2),
        "diameter": diameter, "avg_path_length": round(avg_path, 2),
        "maintenance": {
            "pings_per_round": 2 * len(edges),
            "maint_interval_s": P2P_MAINT_INTERVAL,
            "ping_msgs_per_s": round(2 * len(edges) / P2P_MAINT_INTERVAL, 2),
            "gossip_interval_s": P2P_GOSSIP_INTERVAL,
            "gossip_origin_msgs_per_put": {u: len(adj[u]) for u in adj},
        },
        "gossip_prob": p_f,
        "p_no_copy_by_degree": {u: round((1 - p_f) ** len(adj[u]), 4) for u in adj},
        "lookups": [],
        "counters": {u: peers[u]["stats"] for u in peers},
    }

    if a.lookups:
        for u, v in itertools.permutations(sorted(peers), 2):
            for _ in range(a.lookups):
                r = ask(LOT_PORTS[u], "lookup", {"lot": v, "no_cache": True, "fresh": True, "ttl": 3}, log)
                if r is None:
                    continue
                out["lookups"].append({"from": u, "for": v, "found": r["found"], "hops": r.get("hops"),
                                       "path": r.get("path"), "messages": r.get("messages"),
                                       "shortest": dists[u].get(v)})
        found = [l for l in out["lookups"] if l["found"]]
        if found:
            out["lookup_cost"] = {
                "count": len(found),
                "avg_hops": round(sum(l["hops"] for l in found) / len(found), 2),
                "max_hops": max(l["hops"] for l in found),
                "avg_messages_sent_by_asker": round(sum(l["messages"] for l in found) / len(found), 2),
                "avg_stretch": round(sum(l["hops"] / max(1, l["shortest"] or 1) for l in found) / len(found), 2),
                "success_rate": round(len(found) / len(out["lookups"]), 2),
            }

    if a.json:
        print(json.dumps(out, indent=2))
        return

    print("Overlay: %d nodes, %d edges, %s" % (n, len(edges), "connected" if connected else "NOT connected"))
    print("  edges        : " + ", ".join("%s-%s" % e for e in edges))
    print("  degree       : " + ", ".join("%s=%d" % (u, d) for u, d in out["degree"].items()) + "  (avg %.2f)" % out["avg_degree"])
    print("  diameter     : %d hops   (longest shortest path)" % diameter)
    print("  avg path     : %.2f hops" % avg_path)
    print("Maintenance:")
    m = out["maintenance"]
    print("  pings/round  : %d (one per directed edge) every %.0fs = %.2f msg/s for the whole overlay" % (m["pings_per_round"], m["maint_interval_s"], m["ping_msgs_per_s"]))
    print("  gossip/put   : origin sends degree(n) copies, informed peers forward with p_f=%.1f; re-gossip every %.0fs" % (p_f, P2P_GOSSIP_INTERVAL))
    print("  P(no copy)   : " + ", ".join("%s: (1-%.1f)^%d = %.3f" % (u, p_f, out["degree"][u], p) for u, p in out["p_no_copy_by_degree"].items()))
    if out["lookups"]:
        lc = out.get("lookup_cost", {})
        print("Lookups (owner-only, cache bypassed, TTL 3): %d run, %.0f%% found" % (len(out["lookups"]), 100 * lc.get("success_rate", 0)))
        for l in out["lookups"]:
            print("  %s asks for %s: %s" % (l["from"], l["for"], ("%d hop(s) via %s, %d msg sent by asker" % (l["hops"], "->".join(l["path"]), l["messages"])) if l["found"] else "not found"))
        if lc:
            print("  avg hops %.2f, max %d, avg msgs sent by asker %.2f, avg stretch %.2f" % (lc["avg_hops"], lc["max_hops"], lc["avg_messages_sent_by_asker"], lc["avg_stretch"]))
    out["counters"] = {u: (ask(LOT_PORTS[u], "peers", {}, log) or peers[u])["stats"] for u in peers}
    print("Counters since start:")
    for u, st in out["counters"].items():
        print("  %s: gossip sent %d recv %d fwd %d stale %d | lookups sent %d recv %d dup %d ttl0 %d | pings %d failed %d repairs %d"
              % (u, st["gossip_sent"], st["gossip_recv"], st["gossip_fwd"], st["gossip_stale"], st["lookup_sent"], st["lookup_recv"],
                 st["lookup_dup"], st["lookup_ttl0"], st["pings"], st["ping_failed"], st["repairs"]))


if __name__ == "__main__":
    main()
