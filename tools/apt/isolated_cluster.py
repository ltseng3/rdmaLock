#!/usr/bin/env python3
"""CloudLab dashboard launcher: dedicated MNs and CNs, no node credentials.

The same JSON topology must be present on all nodes. This tool intentionally
refuses to run a CN workload on any configured MN IP address.
"""
import argparse
import ipaddress
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
ID = re.compile(r"^[a-zA-Z][a-zA-Z0-9_-]{0,30}$")


def validate(doc):
    if set(doc) != {"device", "ib_port", "replicas", "data_mn", "clients", "benchmark"}:
        raise ValueError("topology requires exactly device, ib_port, replicas, data_mn, clients, benchmark")
    if not re.fullmatch(r"[a-zA-Z0-9_.-]{1,32}", doc["device"]):
        raise ValueError("invalid IB device name")
    if doc["ib_port"] not in (1, 2):
        raise ValueError("IB port must be 1 or 2")
    if len(doc["replicas"]) != 3 or len(doc["clients"]) < 2:
        raise ValueError("require three replicas and at least two CNs")
    nodes = doc["replicas"] + [doc["data_mn"]] + doc["clients"]
    ids, addresses = set(), set()
    for node in nodes:
        if not ID.fullmatch(node.get("id", "")):
            raise ValueError(f"invalid node ID: {node!r}")
        addr = ipaddress.ip_address(node.get("ip", ""))
        if addr.version != 4 or not addr.is_private:
            raise ValueError("all nodes must use private experiment IPv4 addresses")
        if node["id"] in ids or str(addr) in addresses:
            raise ValueError("MNs and CNs must be physically distinct: duplicate name or IP")
        if node["id"] in ("coordinator", "ordering"):
            raise ValueError("reserved node ID")
        ids.add(node["id"])
        addresses.add(str(addr))
    total = 0
    for node in doc["clients"]:
        n = node.get("workers")
        if not isinstance(n, int) or not (1 <= n <= 16):
            raise ValueError("each CN must have 1..16 worker processes")
        cores = node.get("cores", 8)
        if type(cores) is not int or not (1 <= cores <= 32) or n > cores:
            raise ValueError("CN workers exceed configured physical cores; avoid oversubscribing CPUs")
        total += n
    if total > 64:
        raise ValueError("this executable currently supports at most 64 worker processes")
    bench = doc["benchmark"]
    for key, lo, hi in [("positions", total, 3000), ("ballot", 1, 2**63-1),
                        ("order_batch", 1, 32), ("work_ns", 0, 1000000000),
                        ("probe", 0, 1)]:
        value = bench.get(key)
        if type(value) is not int or not (lo <= value <= hi):
            raise ValueError(f"benchmark.{key} must be an integer from {lo} to {hi}")
    return total


def read(path):
    doc = json.loads(Path(path).read_text())
    n = validate(doc)
    return doc, n


def role_nodes(doc):
    return {n["id"]: n for n in doc["replicas"] + [doc["data_mn"]] + doc["clients"]}


def workers_for(doc, node_id):
    idx = 0
    for cn in doc["clients"]:
        if cn["id"] == node_id:
            return list(range(idx, idx + cn["workers"]))
        idx += cn["workers"]
    raise ValueError(f"not a CN node: {node_id}")


def check_host(ip):
    try:
        result = subprocess.check_output(["ip", "-o", "-4", "addr", "show"], text=True)
    except (FileNotFoundError, subprocess.CalledProcessError) as exc:
        raise RuntimeError("cannot inspect local IP using iproute2") from exc
    if not re.search(r"\binet\s+" + re.escape(ip) + r"/\d+\b", result):
        raise RuntimeError(f"host IP mismatch: {ip} is not assigned to this node; refusing to mix MN/CN roles")


def commands(doc, role, *, ballot=None, positions=None, batch=None, probe=None, work_ns=None):
    total = validate(doc)
    nodes = role_nodes(doc)
    b = dict(doc["benchmark"])
    overrides = {"ballot": ballot, "positions": positions, "order_batch": batch,
                 "probe": probe, "work_ns": work_ns}
    b.update({k: v for k, v in overrides.items() if v is not None})
    copy = dict(doc)
    copy["benchmark"] = b
    validate(copy)
    r = doc["replicas"]
    dat = doc["data_mn"]
    base = [str(ROOT / "mucql" / "build-apt")]
    binary = lambda s: str(Path(base[0]) / s)
    dev = doc["device"]
    if role == "coordinator":
        return [[binary("mucql_lock_e2e"), "coordinator", "29700", str(b["ballot"]),
                 str(b["positions"]), str(total), dat["ip"] + ":29670",
                 *(n["ip"] + ":29660" for n in r), dev, str(b["probe"]), str(b["work_ns"])]]
    if role == "ordering":
        return [[binary("mucql_rdma_paxos"), "client", str(b["ballot"]),
                 str(b["positions"]), *(n["ip"] + ":29660" for n in r),
                 dev, str(b["order_batch"])]]
    if role not in nodes:
        raise ValueError(f"unknown node {role}; valid: {', '.join(nodes)} or coordinator/ordering")
    if role in [n["id"] for n in r]:
        return [[binary("mucql_rdma_paxos"), "server", "29660", dev]]
    if role == dat["id"]:
        return [[binary("mucql_verbs_release"), "server", "29670", dev, "4096"]]
    cn = nodes[role]
    return [[binary("mucql_lock_e2e"), "worker", dat["ip"] + ":29700",
             str(i), cn["ip"], str(29711 + i), dev] for i in workers_for(doc, role)]


def show(doc, total):
    print(f"MNs: 3 quorum replicas plus 1 dedicated data memory node; CNs: {len(doc['clients'])}; worker processes: {total}")
    for n in doc["replicas"]:
        print(f"  {n['id']:12s} {n['ip']:15s} role=replica (NO CLIENTS)")
    d = doc["data_mn"]
    print(f"  {d['id']:12s} {d['ip']:15s} role=data MN + coordinator (NO CLIENTS)")
    for n in doc["clients"]:
        print(f"  {n['id']:12s} {n['ip']:15s} role=CN workers={n['workers']} ids={workers_for(doc,n['id'])}")
    print("NOTICE: Current E2E is PRECOMMITTED fixed-batch, not continuous ordering+handoff; not DecLock-comparable goodput.")


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--config", default=str(ROOT / "tools/apt/topology.json"))
    sub = p.add_subparsers(dest="action", required=True)
    sub.add_parser("show")
    for name in ("print", "run"):
        a = sub.add_parser(name)
        a.add_argument("role", help="node ID, coordinator or ordering")
        a.add_argument("--ballot", type=int)
        a.add_argument("--positions", type=int)
        a.add_argument("--batch", type=int)
        a.add_argument("--probe", type=int, choices=(0, 1))
        a.add_argument("--work-ns", type=int)
        if name == "run":
            a.add_argument("--skip-ip-check", action="store_true", help="debug only")
            a.add_argument("--log-dir", default="results/isolated")
    args = p.parse_args()
    doc, total = read(args.config)
    if args.action == "show":
        show(doc, total)
        return
    cmds = commands(doc, args.role, ballot=args.ballot, positions=args.positions,
                    batch=args.batch, probe=args.probe, work_ns=args.work_ns)
    env = dict(os.environ, MUCQL_IB_PORT=str(doc["ib_port"]))
    for line in cmds:
        print("MUCQL_IB_PORT=" + str(doc["ib_port"]) + " " + shlex.join(line), flush=True)
    if args.action == "print":
        return
    own = doc["data_mn"] if args.role in ("coordinator", "ordering") else role_nodes(doc)[args.role]
    if args.role == "ordering":
        # Ordering driver is on the dedicated data MN, not on any CN.
        own = doc["data_mn"]
    if not args.skip_ip_check:
        check_host(own["ip"])
    # Never run a client process on a memory/replica node.
    if args.role in (n["id"] for n in doc["replicas"] + [doc["data_mn"]]):
        subprocess.run(cmds[0], check=True, env=env)
        return
    if args.role in ("coordinator", "ordering"):
        if args.role == "coordinator":
            env["MUCQL_ORDER_BATCH"] = str(args.batch or doc["benchmark"]["order_batch"])
        subprocess.run(cmds[0], check=True, env=env)
        return
    logdir = Path(args.log_dir)
    logdir.mkdir(parents=True, exist_ok=True)
    children = []
    try:
        for line in cmds:
            idx = line[3]
            log = open(logdir / f"worker_{idx}.log", "w")
            child = subprocess.Popen(line, env=env, stdout=log, stderr=subprocess.STDOUT)
            children.append((child, log, idx))
            time.sleep(.015)
        errors = []
        for child, _, idx in children:
            rc = child.wait()
            if rc:
                errors.append((idx, rc))
        if errors:
            raise RuntimeError(f"worker failures: {errors}; see {logdir}/worker_*.log")
        print(f"All {len(children)} local workers PASS")
    finally:
        for child, log, _ in children:
            if child.poll() is None:
                child.terminate()
            log.close()


if __name__ == "__main__":
    try:
        main()
    except (ValueError, RuntimeError, subprocess.CalledProcessError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(2)
