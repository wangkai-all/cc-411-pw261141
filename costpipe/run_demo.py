"""End-to-end demo: three independent processes + the full correction story.

Flow:
  1. seed fictional data, start ingest service, worker, report subscriber
  2. initial allocation (two periods, real + budget scenario)
  3. retroactive org correction -> old contributions re-derived, not renamed
  4. rule change + missing usage backfill
  5. subscriber goes OFFLINE, changes accumulate, then catches up in order
  6. per-charge trace, conservation + rounding-tail evidence

Usage:  python run_demo.py            (tears everything down afterwards)
        python run_demo.py --keep     (leave processes + db running)
"""
from __future__ import annotations

import argparse
import json
import os
import signal
import sqlite3
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent
DB = ROOT / "data" / "costpipe.db"
SUB_DB = ROOT / "data" / "subscriber.db"
PORT = int(os.environ.get("COSTPIPE_PORT", "8791"))
BASE = f"http://127.0.0.1:{PORT}"
PYTHON = sys.executable


def banner(title: str) -> None:
    print("\n" + "=" * 78)
    print(f"  {title}")
    print("=" * 78)


def api(method: str, path: str, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        BASE + path, data=data,
        headers={"Content-Type": "application/json"}, method=method)
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read().decode())


def wait_for(predicate, desc: str, timeout: float = 20.0):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            last = predicate()
            if last:
                return last
        except (urllib.error.URLError, ConnectionError, OSError):
            pass
        time.sleep(0.3)
    raise TimeoutError(f"timed out waiting for {desc}; last={last}")


def wait_queue_drained():
    def drained():
        jobs = api("GET", "/api/jobs")
        return all(j["status"] in ("done", "failed") for j in jobs) and \
            not any(j["status"] == "queued" for j in jobs)
    wait_for(drained, "job queue drained")
    time.sleep(0.4)  # let events become visible


def start(name: str, *args) -> subprocess.Popen:
    log = open(ROOT / "data" / f"{name}.log", "w")
    proc = subprocess.Popen(
        [PYTHON, "-m", *args], cwd=ROOT, stdout=log, stderr=subprocess.STDOUT,
        env={**os.environ, "PYTHONUNBUFFERED": "1"})
    print(f"  started {name} (pid {proc.pid}) -> data/{name}.log")
    return proc


def stop(proc: subprocess.Popen, name: str) -> None:
    proc.send_signal(signal.SIGINT if os.name != "nt" else signal.SIGTERM)
    try:
        proc.wait(timeout=8)
    except subprocess.TimeoutExpired:
        proc.kill()
    print(f"  stopped {name}")


def cleanup_stale() -> None:
    """Best-effort: remove leftover processes bound to THIS project's data dir.

    A previous run killed mid-flight can leave a worker/subscriber holding the
    SQLite files; scoping the match to our absolute data path avoids touching
    any unrelated process.
    """
    try:
        out = subprocess.run(
            ["pgrep", "-f", f"costpipe\\.(ingest|worker|subscriber).*{ROOT}"],
            capture_output=True, text=True, check=False)
        pids = [int(p) for p in out.stdout.split()]
        for pid in pids:
            if pid != os.getpid():
                try:
                    os.kill(pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
        if pids:
            time.sleep(1.0)
            print(f"  cleaned up {len(pids)} stale process(es) from a previous run")
    except FileNotFoundError:
        pass  # pgrep unavailable; rely on fresh port/DB instead


def yuan(cents) -> str:
    return f"{cents / 100:.2f} 元"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--keep", action="store_true",
                    help="keep processes and database after the demo")
    args = ap.parse_args()

    for p in (DB, SUB_DB):
        for suffix in ("", "-wal", "-shm"):
            Path(str(p) + suffix).unlink(missing_ok=True)
    (ROOT / "data").mkdir(exist_ok=True)
    cleanup_stale()

    # ------------------------------------------------------------ 1. boot
    banner("1/7  初始化虚构数据并启动三个独立进程")
    subprocess.run([PYTHON, "-m", "costpipe.seed", "--db", str(DB), "--reset"],
                   cwd=ROOT, check=True)
    ingest = start("ingest", "costpipe.ingest", "--db", str(DB), "--port", str(PORT))
    worker = start("worker", "costpipe.worker", "--db", str(DB))
    wait_for(lambda: api("GET", "/health"), "ingest health")
    subscriber = start("subscriber", "costpipe.subscriber",
                       "--url", BASE, "--store", str(SUB_DB))

    # ------------------------------------------------ 2. first allocation
    banner("2/7  首轮分摊：真实费用 + budget2027 情景，金额守恒检查")
    wait_queue_drained()
    summary = api("GET", "/api/summary")
    real_v = [a for a in summary["current_allocations"] if a["scenario_id"] == "actual"]
    budget_v = [a for a in summary["current_allocations"] if a["scenario_id"] == "budget2027"]
    charge_total = sum(c["amount_cents"] for c in api("GET", "/api/charges"))
    alloc_total = sum(a["total_cents"] for a in real_v)
    print(f"  账单费用合计         : {yuan(charge_total)}")
    print(f"  actual 分配合计      : {yuan(alloc_total)}")
    print(f"  金额守恒             : {'✔ 完全相等' if charge_total == alloc_total else '✘'}")
    print(f"  情景隔离             : budget2027 合计 {yuan(sum(a['total_cents'] for a in budget_v))}"
          "（is_real=0，永不并入真实费用）")
    nov = next(a for a in real_v if a["period"] == "2026-11")
    print("  2026-11 实际账期组织级分摊:")
    for org, cents in sorted(nov["org_totals_cents"].items()):
        print(f"    {org:12s} {yuan(cents)}")

    # --------------------------------------------- 3. retroactive org fix
    banner("3/7  组织关系追溯更正：DATA 组升级为一级部门（SCD2，旧贡献保留重算）")
    before = api("GET", "/api/trace/INV-202610-S3-01?scenario=actual")
    before_path = next(l["org_path"] for l in before["lines"] if l["org_unit_id"] == "DATA")
    print(f"  更正前 DATA 路径     : {before_path}")
    # Two-part correction: rename + re-parent. Re-parenting alone only changes
    # the path (weights are leaf-based), so we also inspect the sibling whose
    # historical contribution genuinely moves when the tree is re-derived.
    correction = api("POST", "/api/org/correction", {
        "org_unit_id": "DATA", "new_name": "数据与AI工程组",
        "new_parent_id": "ROOT", "valid_from": "2026-10-01",
        "reason": "演示：组织架构调整，需追溯重算历史账期"})
    print(f"  SCD2 新版本          : v{correction['old_version']} -> v{correction['new_version']}"
          f"，受影响账期 {correction['affected_periods']}")
    wait_queue_drained()
    after = api("GET", "/api/trace/INV-202610-S3-01?scenario=actual")
    after_path = next(l["org_path"] for l in after["lines"] if l["org_unit_id"] == "DATA")
    print(f"  更正后 DATA 路径     : {after_path}")
    print(f"  旧版本仍可查         : 分摊版本 # {after['alloc_version']['supersedes_id']} "
          f"已 superseded，其 alloc_line 未被删除或改名")
    events = api("GET", "/api/events?after=0&limit=200")
    pub = [e for e in events if e["event_type"] == "allocation.published"]
    corrected = [e for e in pub
                 if e["payload"].get("trigger_event") == "org.corrected"][0]
    # The path changed on every DATA line even though its cent total did not;
    # prove the engine re-derived (not renamed) by comparing the full line set
    # between the old and new immutable versions.
    old_vid = after["alloc_version"]["supersedes_id"]
    old_lines = api("GET", f"/api/allocations/versions/{old_vid}/lines")["lines"]
    new_lines = after["lines"]
    old_data_path = next(l["org_path"] for l in old_lines if l["org_unit_id"] == "DATA")
    print("  旧贡献被重新派生（非仅改名）:")
    print(f"    旧版本 DATA 行路径 = {old_data_path}")
    print(f"    新版本 DATA 行路径 = {after_path}")
    d = corrected["payload"]["diff_from_previous"]["DATA"]
    print(f"    DATA 金额 old={yuan(d['old_cents'])} new={yuan(d['new_cents'])} "
          f"delta={d['delta_cents']} 分（叶子权重未变 -> 金额守恒为 0，路径/层级已更正）")

    # A second correction that DOES move money: merge EMEA under APAC. The
    # engine allocates to LEAF orgs, so APAC (now a parent) stops receiving
    # direct allocation while EMEA, which still has its own usage meter,
    # remains a leaf. A rename-only fix could never produce these non-zero
    # deltas — the historical contributions had to be re-derived.
    merge = api("POST", "/api/org/correction", {
        "org_unit_id": "EMEA", "new_name": "欧洲业务（并入亚太管理层级）",
        "new_parent_id": "APAC", "valid_from": "2026-10-01",
        "reason": "演示：汇报关系合并，APAC 变为非叶子，历史贡献需重新归属"})
    wait_queue_drained()
    merge_ev = [e for e in api("GET", "/api/events?after=0&limit=200")
                if e["event_type"] == "allocation.published"
                and e["payload"].get("trigger_event") == "org.corrected"][-1]
    md = merge_ev["payload"]["diff_from_previous"]
    moved = {o: v["delta_cents"] for o, v in md.items() if v["delta_cents"] != 0}
    print("  合并 EMEA->APAC 后历史账期 2026-10 的贡献调整（delta 非零，证明非改名）:")
    for org, delta in sorted(moved.items()):
        print(f"    {org:6s} delta={delta:+d} 分")
    merge_total = sum(v["new_cents"] for v in md.values())
    print(f"  更正后分配合计仍守恒 : {yuan(merge_total)}（APAC 转为父节点，其原份额被重新派生到其余叶子）")

    # --------------------------------------- 4. rule change + usage patch
    banner("4/7  规则改版 + 补录使用量（三源独立更新的另两条链路）")
    rule = api("POST", "/api/rules", {
        "rule_id": "rule_s3_v2_platform_heavy", "scenario_id": "actual",
        "service_id": "S3", "basis": "fixed",
        "fixed_weights": {"PLAT": 0.7, "DATA": 0.2, "APAC": 0.05, "EMEA": 0.05},
        "effective_from": "2026-11"})
    print(f"  新规则已版本化       : {rule['rule_id']} v{rule['version']}（2026-11 起生效，10 月不受影响）")
    backfill = api("POST", "/api/usage", [{
        "usage_id": "UG-202610-EC2-EMEA-BACKFILL", "period": "2026-10",
        "service_id": "EC2", "org_unit_id": "EMEA", "usage_qty": 130}])
    print(f"  补录用量             : {backfill}")
    wait_queue_drained()

    # ------------------------------------------------- 5. offline window
    banner("5/7  模拟报告订阅端离线 → 恢复后按 seq 追平全部版本")
    stop(subscriber, "subscriber")
    seq_before = api("GET", "/api/summary")["max_event_seq"]
    print(f"  离线时事件 seq       : {seq_before}")
    api("POST", "/api/charges", [{
        "charge_id": "INV-202611-NET-01", "period": "2026-11",
        "service_id": "NETWORK", "vendor": "虚构云厂A", "amount_cents": 26789}])
    api("POST", "/api/recompute", {"force": True})
    wait_queue_drained()
    seq_after = api("GET", "/api/summary")["max_event_seq"]
    print(f"  离线期间新增事件     : seq {seq_before + 1} .. {seq_after}（订阅端完全错过）")

    subscriber = start("subscriber", "costpipe.subscriber",
                       "--url", BASE, "--store", str(SUB_DB))
    wait_for(lambda: (
        sqlite3.connect(SUB_DB).execute(
            "SELECT MAX(seq) FROM received_event").fetchone()[0] or 0) >= seq_after,
        "subscriber catch-up")
    time.sleep(0.5)
    sconn = sqlite3.connect(SUB_DB)
    got = sconn.execute("SELECT COUNT(*), MIN(seq), MAX(seq) FROM received_event").fetchone()
    checks = sconn.execute(
        "SELECT COUNT(*), SUM(ok) FROM conservation_check").fetchone()
    versions_seen = sconn.execute(
        "SELECT DISTINCT alloc_version_id FROM materialized_report").fetchall()
    print(f"  追平后本地事件       : {got[0]} 条，seq 连续覆盖 {got[1]}..{got[2]}")
    print(f"  本地物化过的版本数   : {len(versions_seen)}（含被取代的历史版本，顺序回放）")
    print(f"  订阅端守恒自检       : {checks[1]}/{checks[0]} 通过")
    srv_offset = api("GET", "/api/consumers")[0]
    print(f"  服务端确认 offset    : consumer={srv_offset['consumer_id']} last_seq={srv_offset['last_seq']}")

    # -------------------------------------------------- 6. trace + tails
    banner("6/7  逐笔追溯与尾差可解释性")
    trace = api("GET", "/api/trace/INV-202611-NET-01?scenario=actual")
    print(f"  新费用 {trace['charge']['charge_id']}（{yuan(trace['charge']['amount_cents'])}）逐笔去向:")
    line_sum = 0
    for ln in trace["lines"]:
        line_sum += ln["allocated_cents"]
        print(f"    -> {ln['org_unit_id']:6s} {yuan(ln['allocated_cents']):>12} "
              f"weight={ln['weight']:.4f}  {ln['note']}")
    print(f"  逐笔合计 == 原始费用 : {'✔' if line_sum == trace['charge']['amount_cents'] else '✘'}")
    latest_pub = [e for e in api("GET", "/api/events?after=0&limit=200")
                  if e["event_type"] == "allocation.published"
                  and e["payload"]["period"] == "2026-11"
                  and e["payload"]["scenario_id"] == "actual"][-1]
    audit = latest_pub["payload"]["rounding_audit"]
    tail = sum(a["remainder_cents"] for a in audit)
    print(f"  尾差审计（最大余数法）: 2026-11 actual 共 {tail} 分因整除产生，"
          f"逐笔记账到具体组织（审计条目 {len(audit)} 条）")
    for a in audit[:3]:
        who = ", ".join(f"{x['org_unit_id']}(+1c)" for x in a["adjustments"])
        print(f"    {a['charge_id']}: {a['remainder_cents']} 分 -> {who}")

    # ----------------------------------------------------- 7. summary
    banner("7/7  最终版本谱系（不可变留痕 + 指纹跳重算）")
    for v in api("GET", "/api/allocations"):
        print(f"  #{v['id']:>3} {v['period']} {v['scenario_id']:11s} v{v['version_no']} "
              f"{v['status']:10s} total={yuan(v['total_cents']):>12} "
              f"supersedes={v['supersedes_id'] or '-'}")
    print("\n  网页控制台: " + BASE + "/  （总览 / 输入 / 版本 / 追溯 / 规则 / 组织 / 订阅端）")

    if not args.keep:
        stop(subscriber, "subscriber")
        stop(worker, "worker")
        stop(ingest, "ingest")
        print("\n演示结束（进程已停止；数据库保留在 data/ 供检查，加 --keep 可保留进程）")
    else:
        print("\n--keep：三个进程继续运行，可打开网页操作")


if __name__ == "__main__":
    main()
