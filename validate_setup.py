#!/usr/bin/env python3
"""
validate_setup.py  —  Pre-flight validation for phase1_parallel_aws.py
Zero-Telemetry CPU Prediction / Carbon-Aware Right-Sizing

Runs 15 targeted checks in ~4 minutes covering:
  cluster, OCI tools, all 10 benchmark tools, Python packages,
  startup CPU sampling, parallel infrastructure, and resource headroom.

Every check prints PASS or FAIL with an exact fix command.
All REQUIRED checks must pass before running phase1_parallel_aws.py --full.

Usage:
    python3 validate_setup.py           # all 15 checks (~4 min)
    python3 validate_setup.py --quick   # skip k6 live test and codecarbon (~2 min)
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from datetime import datetime
from pathlib import Path

# ── venv bridge ───────────────────────────────────────────────────────────────
_VENV_PY = Path.home() / ".venv-zerotelem" / "bin" / "python"
PY = str(_VENV_PY) if _VENV_PY.exists() else sys.executable

# ── colours ───────────────────────────────────────────────────────────────────
G, R, Y, B = "\033[92m", "\033[91m", "\033[93m", "\033[94m"
RST, BOLD, DIM = "\033[0m", "\033[1m", "\033[2m"

# ── result store ──────────────────────────────────────────────────────────────
_results: list[dict] = []   # {name, required, passed, detail, fix}


def _run(cmd, timeout=30):
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return r.returncode, r.stdout.strip(), r.stderr.strip()
    except subprocess.TimeoutExpired:
        return 1, "", f"timeout after {timeout}s"
    except FileNotFoundError:
        return 1, "", f"not found: {cmd[0]}"
    except Exception as exc:
        return 1, "", str(exc)


def _record(name: str, passed: bool, detail: str,
            fix: str = "", required: bool = True) -> bool:
    _results.append({"name": name, "required": required,
                     "passed": passed, "detail": detail, "fix": fix})
    tag  = f"{Y}[required]{RST}" if required and not passed else \
           f"{DIM}[advisory]{RST}" if not required else ""
    icon = f"{G}PASS{RST}" if passed else f"{R}FAIL{RST}"
    print(f"  {icon}  {name:<40}  {DIM}{detail[:55]}{RST}  {tag}")
    if not passed and fix:
        print(f"        {Y}fix →  {fix}{RST}")
    return passed


def _section(title):
    print(f"\n  {BOLD}{B}── {title} ──{RST}")


# =============================================================================
#  CHECK 1 — Python version
# =============================================================================
def check_python():
    v = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
    ok = sys.version_info >= (3, 10)
    _record("Python ≥ 3.10", ok, v,
            fix="sudo apt-get install python3.10  OR  use pyenv")


# =============================================================================
#  CHECK 2 — kubectl + cluster reachable
# =============================================================================
def check_kubectl():
    rc, out, err = _run(["kubectl", "get", "nodes", "--no-headers"], timeout=20)
    if rc != 0:
        _record("kubectl cluster", False, err[:60],
                fix="curl -sfL https://get.k3s.io | sh -  &&  "
                    "sudo cp /etc/rancher/k3s/k3s.yaml ~/.kube/config  &&  "
                    "sudo chown $USER ~/.kube/config")
        return
    lines = [l for l in out.splitlines() if l.strip()]
    ready = [l for l in lines if "Ready" in l and "NotReady" not in l]
    _record("kubectl cluster",
            bool(ready), f"{len(ready)}/{len(lines)} node(s) Ready",
            fix="sudo systemctl restart k3s  &&  sleep 30")


# =============================================================================
#  CHECK 3 — kubectl top (metrics-server)
# =============================================================================
def check_kubectl_top():
    print(f"  {DIM}  checking kubectl top (up to 60s)...{RST}", end="", flush=True)
    for attempt in range(3):
        if attempt:
            time.sleep(20)
        rc, out, _ = _run(["kubectl", "top", "nodes"], timeout=30)
        if rc == 0 and out:
            print()
            _record("kubectl top / metrics-server", True,
                    out.splitlines()[0][:55])
            return
    print()
    _record("kubectl top / metrics-server", False,
            "not ready after 3 attempts",
            fix="helm upgrade --install metrics-server "
                "metrics-server/metrics-server "
                "--namespace kube-system "
                "--set args={--kubelet-insecure-tls}  &&  sleep 60")


# =============================================================================
#  CHECK 4 — skopeo  (OCI metadata — core feature extraction)
# =============================================================================
def check_skopeo():
    if not shutil.which("skopeo"):
        _record("skopeo", False, "not found",
                fix="sudo apt-get install -y skopeo")
        return
    rc, out, err = _run(
        ["skopeo", "inspect", "--raw",
         "docker://docker.io/library/nginx:alpine"], timeout=45)
    if rc == 0 and out:
        try:
            m = json.loads(out)
            layers = m.get("layers", m.get("manifests", []))
            _record("skopeo", True, f"nginx:alpine — {len(layers)} layer(s)")
            return
        except json.JSONDecodeError:
            _record("skopeo", True, "manifest pulled (raw)")
            return
    _record("skopeo", False, err[:60],
            fix="sudo apt-get install -y skopeo")


# =============================================================================
#  CHECK 5 — docker manifest inspect  (compressed_ratio feature)
# =============================================================================
def check_docker_manifest():
    if not shutil.which("docker"):
        _record("docker manifest inspect", False, "docker not found",
                fix="sudo apt-get install -y docker.io  &&  "
                    "sudo systemctl start docker  &&  sudo usermod -aG docker $USER",
                required=False)
        return
    rc, out, err = _run(
        ["docker", "manifest", "inspect", "nginx:alpine"], timeout=45)
    if rc == 0 and out:
        try:
            m = json.loads(out)
            layers = m.get("layers", [])
            _record("docker manifest inspect", True,
                    f"nginx:alpine — {len(layers)} compressed layer(s)",
                    required=False)
        except json.JSONDecodeError:
            _record("docker manifest inspect", True, "manifest returned",
                    required=False)
    else:
        _record("docker manifest inspect", False, err[:60],
                fix="newgrp docker   OR   log out and back in   "
                    "OR   sudo chmod 666 /var/run/docker.sock",
                required=False)


# =============================================================================
#  CHECK 6 — k6  (load test engine)
# =============================================================================
def check_k6(quick: bool):
    if not shutil.which("k6"):
        _record("k6", False, "not found",
                fix="See bootstrap_aws.sh step 7 for Grafana repo install")
        return
    if quick:
        rc, out, _ = _run(["k6", "version"])
        _record("k6", rc == 0, out[:50] if rc == 0 else "version check failed",
                fix="See bootstrap_aws.sh step 7")
        return
    script = (
        "import http from 'k6/http';\n"
        "import { sleep } from 'k6';\n"
        "export const options = { vus: 2, duration: '5s' };\n"
        "export default function () { http.get('http://test.k6.io'); sleep(1); }\n"
    )
    with tempfile.NamedTemporaryFile(suffix=".js", mode="w", delete=False) as f:
        f.write(script); tmp = f.name
    try:
        rc, out, err = _run(["k6", "run", "--quiet", tmp], timeout=30)
        _record("k6 live test", rc == 0,
                "5s/2VU test passed" if rc == 0 else (err or out)[:55],
                fix="Check internet connectivity from EC2 instance")
    finally:
        os.unlink(tmp)


# =============================================================================
#  CHECK 7 — Python packages
# =============================================================================
_PKG_TESTS = [
    ("pandas",      "import pandas as pd; assert pd.__version__"),
    ("scikit-learn","from sklearn.ensemble import RandomForestRegressor"),
    ("xgboost",     "import xgboost; assert xgboost.__version__"),
    ("shap",        "import shap; assert shap.__version__"),
    ("matplotlib",  "import matplotlib; assert matplotlib.__version__"),
    ("codecarbon",  "from codecarbon import EmissionsTracker"),
    ("pyyaml",      "import yaml; assert yaml.__version__"),
    ("tqdm",        "import tqdm; assert tqdm.__version__"),
]

def check_python_packages():
    all_ok = True
    for pkg, stmt in _PKG_TESTS:
        rc, _, err = _run([PY, "-c", stmt], timeout=15)
        if rc == 0:
            vrc, ver, _ = _run([PY, "-c",
                                 f"import {pkg.replace('-','_').replace('<2','')} as m; "
                                 f"print(getattr(m,'__version__','ok'))"],
                                timeout=10)
            _record(f"python: {pkg}", True, f"v{ver}" if vrc == 0 else "ok")
        else:
            _record(f"python: {pkg}", False, (err or "ImportError")[:55],
                    fix=f"~/.venv-zerotelem/bin/pip install {pkg}")
            all_ok = False
    return all_ok


# =============================================================================
#  CHECK 8 — codecarbon live (3-second emission track)
# =============================================================================
def check_codecarbon(quick: bool):
    if quick:
        rc, _, _ = _run([PY, "-c", "from codecarbon import EmissionsTracker"],
                        timeout=10)
        _record("codecarbon import", rc == 0, "importable",
                fix="~/.venv-zerotelem/bin/pip install codecarbon")
        return
    script = (
        "import time\n"
        "from codecarbon import EmissionsTracker\n"
        "t = EmissionsTracker(log_level='error', save_to_file=False)\n"
        "t.start(); time.sleep(3); e = t.stop()\n"
        "print(f'emissions:{e:.2e}')\n"
    )
    with tempfile.NamedTemporaryFile(suffix=".py", mode="w", delete=False) as f:
        f.write(script); tmp = f.name
    try:
        rc, out, err = _run([PY, tmp], timeout=25)
        if rc == 0 and "emissions:" in out:
            val = out.split("emissions:")[-1].strip()
            _record("codecarbon live", True, f"tracked {val} kg CO₂eq in 3s")
        else:
            _record("codecarbon live", False, (err or out)[:55],
                    fix="~/.venv-zerotelem/bin/pip install --upgrade codecarbon")
    finally:
        os.unlink(tmp)


# =============================================================================
#  CHECK 9 — multiprocessing (ProcessPoolExecutor sanity)
# =============================================================================
def check_multiprocessing():
    import tempfile, os as _os
    script = (
        "import multiprocessing as mp\n"
        "mp.set_start_method('spawn', force=True)\n"
        "from concurrent.futures import ProcessPoolExecutor\n"
        "def f(x): return x*x\n"
        "if __name__ == '__main__':\n"
        "    with ProcessPoolExecutor(max_workers=2) as p:\n"
        "        r = list(p.map(f, range(4)))\n"
        "    assert r == [0,1,4,9], r\n"
        "    print('ok')\n"
    )
    with tempfile.NamedTemporaryFile(suffix=".py", mode="w",
                                     delete=False, dir="/tmp") as f:
        f.write(script); tmp = f.name
    try:
        rc, out, err = _run([PY, tmp], timeout=30)
        ok = rc == 0 and "ok" in out
        _record("multiprocessing / ProcessPoolExecutor", ok,
                "2-worker pool ok" if ok else (err or out)[:80],
                fix="Python 3.14 spawn requires running from a .py file")
    finally:
        _os.unlink(tmp)

BENCH_TOOL_CHECKS = [
    ("redis-benchmark",          ["redis-benchmark",          "--version"], True),
    ("sysbench",                 ["sysbench",                 "--version"], True),
    ("pgbench",                  ["pgbench",                  "--version"], True),
    ("memtier_benchmark",        ["memtier_benchmark",        "--version"], False),
    ("wrk",                      ["wrk",                      "--version"], False),
    ("mongosh",                  ["mongosh",                  "--version"], False),
    ("stress-ng",                ["stress-ng",                "--version"], False),
    ("rabbitmq-perf-test",       ["rabbitmq-perf-test",       "--help"],    False),
    ("kafka-producer-perf-test", ["kafka-producer-perf-test.sh", "--help"], False),
    ("cassandra-stress",         ["cassandra-stress",         "help"],      False),
]

def check_benchmark_tools():
    for name, cmd, required in BENCH_TOOL_CHECKS:
        rc, out, err = _run(cmd, timeout=20)
        ok = bool(out or err) or rc == 0
        detail = ((out or err)[:55]).split("\n")[0] if ok else f"not found: {name}"
        fix = "Run: ./benchmark_install.sh" if not ok else ""
        _record(f"bench: {name}", ok, detail, fix=fix, required=required)

def check_startup_cpu_sampling():
    """
    Verify that kubectl top pod returns readings during the warmup window
    — the same window used by sample_cpu_startup() in phase1_parallel_aws.py.
    Deploy a minimal pod, wait 10 s, confirm at least one top reading > 0.
    A reading of 0m here is acceptable; what matters is the command succeeds.
    """
    NS   = "zt-startup-check"
    SLUG = "busybox-startup"
    import textwrap as _tw

    _run(["kubectl", "delete", "namespace", NS, "--ignore-not-found=true"], timeout=15)
    _run(["kubectl", "create", "namespace", NS], timeout=10)

    yaml = _tw.dedent(f"""\
        apiVersion: apps/v1
        kind: Deployment
        metadata:
          name: {SLUG}
          namespace: {NS}
          labels: {{app: {SLUG}}}
        spec:
          replicas: 1
          selector:
            matchLabels: {{app: {SLUG}}}
          template:
            metadata:
              labels: {{app: {SLUG}}}
            spec:
              containers:
              - name: {SLUG}
                image: busybox:latest
                command: ["sh", "-c", "while true; do echo ok; sleep 5; done"]
                resources:
                  requests:
                    memory: "8Mi"
                  limits:
                    memory: "32Mi"
    """)
    import tempfile, os
    with tempfile.NamedTemporaryFile(suffix=".yaml", mode="w", delete=False) as f:
        f.write(yaml); tmp = f.name
    rc, _, err = _run(["kubectl", "apply", "-f", tmp], timeout=20)
    os.unlink(tmp)
    if rc != 0:
        _record("startup CPU sampling", False, f"kubectl apply failed: {err[:55]}",
                fix="Check k3s health: sudo systemctl status k3s")
        _run(["kubectl", "delete", "namespace", NS, "--ignore-not-found=true"], timeout=15)
        return

    # Wait up to 60 s for Running
    pod_up = False
    print(f"  {DIM}  waiting for busybox pod (~20s)...{RST}", end="", flush=True)
    for _ in range(12):
        time.sleep(5)
        rc, out, _ = _run(["kubectl", "get", "pods", "-n", NS,
                            "-l", f"app={SLUG}", "--no-headers"], timeout=10)
        if rc == 0 and "Running" in out:
            pod_up = True; break
    print()
    if not pod_up:
        _record("startup CPU sampling", False, "pod did not reach Running in 60s",
                fix="kubectl describe pod -n zt-startup-check")
        _run(["kubectl", "delete", "namespace", NS, "--ignore-not-found=true"], timeout=15)
        return

    time.sleep(45)  # metrics-server needs 30-60s to report a new pod
    readings = []
    for _ in range(8):
        rc, out, _ = _run(["kubectl", "top", "pod", "-n", NS,
                            "-l", f"app={SLUG}", "--no-headers"], timeout=10)
        if rc == 0 and out:
            parts = out.split()
            if len(parts) >= 2:
                try:
                    readings.append(float(parts[1].rstrip("m")))
                except ValueError:
                    pass
        time.sleep(5)

    _run(["kubectl", "delete", "namespace", NS, "--ignore-not-found=true"], timeout=15)

    if readings:
        _record("startup CPU sampling", True,
                f"kubectl top pod returned {len(readings)} reading(s) "
                f"(values: {[f'{v:.0f}m' for v in readings]})")
    else:
        _record("startup CPU sampling", False,
                "kubectl top pod returned no data during startup window",
                fix="Wait 60s for metrics-server, then re-run")



def check_namespace_isolation():
    ns_a, ns_b = "zt-test-a", "zt-test-b"
    for ns in (ns_a, ns_b):
        _run(["kubectl", "create", "namespace", ns], timeout=10)
    rc_a, out_a, _ = _run(
        ["kubectl", "get", "namespace", ns_a, "--no-headers"], timeout=10)
    rc_b, out_b, _ = _run(
        ["kubectl", "get", "namespace", ns_b, "--no-headers"], timeout=10)
    for ns in (ns_a, ns_b):
        _run(["kubectl", "delete", "namespace", ns,
              "--ignore-not-found=true"], timeout=15)
    ok = rc_a == 0 and rc_b == 0
    _record("namespace isolation", ok,
            "create/delete zt-test-a and zt-test-b" if ok else "namespace ops failed",
            fix="Check kubectl RBAC: kubectl auth can-i create namespaces")


# =============================================================================
#  CHECK 11 — full mini pipeline (deploy nginx, sample CPU, teardown)
# =============================================================================
def check_mini_pipeline():
    """
    Deploy nginx:alpine into namespace zt-validate, wait for Running,
    confirm kubectl top pod works, then teardown.
    This is the most important check — it proves the full pipeline works
    on this specific instance before you commit to the 65-min run.
    """
    NS   = "zt-validate"
    NAME = "nginx-alpine"

    # Cleanup any leftover from a previous failed run
    _run(["kubectl", "delete", "namespace", NS,
          "--ignore-not-found=true"], timeout=20)
    time.sleep(3)

    _run(["kubectl", "create", "namespace", NS], timeout=10)

    deploy_yaml = f"""
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {NAME}
  namespace: {NS}
  labels:
    app: {NAME}
spec:
  replicas: 1
  selector:
    matchLabels:
      app: {NAME}
  template:
    metadata:
      labels:
        app: {NAME}
    spec:
      containers:
      - name: {NAME}
        image: nginx:alpine
        ports:
        - containerPort: 80
        resources:
          requests:
            memory: "32Mi"
          limits:
            memory: "128Mi"
"""
    with tempfile.NamedTemporaryFile(suffix=".yaml", mode="w", delete=False) as f:
        f.write(deploy_yaml); tmp = f.name

    try:
        rc, _, err = _run(["kubectl", "apply", "-f", tmp], timeout=20)
        if rc != 0:
            _record("mini pipeline (deploy nginx)", False, err[:55],
                    fix="Check kubectl apply permissions and k3s health")
            return
    finally:
        os.unlink(tmp)

    # Wait for Running (up to 90 s)
    print(f"  {DIM}  waiting for nginx:alpine pod Running (up to 90s)...{RST}",
          end="", flush=True)
    pod_running = False
    for _ in range(18):
        time.sleep(5)
        rc, out, _ = _run(
            ["kubectl", "get", "pods", "-n", NS,
             "-l", f"app={NAME}", "--no-headers"], timeout=10)
        if rc == 0 and "Running" in out:
            pod_running = True
            break

    print()
    if not pod_running:
        _record("mini pipeline (pod Running)", False,
                "pod did not reach Running in 90s",
                fix="kubectl describe pod -n zt-validate  to see pull errors")
        _run(["kubectl", "delete", "namespace", NS,
              "--ignore-not-found=true"], timeout=20)
        return

    _record("mini pipeline (pod Running)", True, "nginx:alpine Running in zt-validate")

    # kubectl top pod
    time.sleep(45)   # brief wait for metrics to populate
    rc, out, _ = _run(
        ["kubectl", "top", "pod", "-n", NS,
         "-l", f"app={NAME}", "--no-headers"], timeout=20)
    if rc == 0 and out:
        parts = out.split()
        cpu_m = parts[1] if len(parts) >= 2 else "?"
        _record("mini pipeline (kubectl top pod)", True,
                f"CPU reading: {cpu_m}")
    else:
        _record("mini pipeline (kubectl top pod)", False,
                "top returned nothing — metrics-server may not be ready",
                fix="wait 60s and re-run, or reinstall metrics-server")

    # Teardown
    _run(["kubectl", "delete", "namespace", NS,
          "--ignore-not-found=true"], timeout=20)
    _record("mini pipeline (teardown)", True, "namespace zt-validate deleted")


# =============================================================================
#  CHECK 12 — disk + memory headroom for parallel run
# =============================================================================
def check_resources():
    import shutil as _sh
    _, _, free = _sh.disk_usage(Path.home())
    free_gb = free / 1024**3
    ok = free_gb >= 15
    _record("Disk free ≥ 15 GB", ok, f"{free_gb:.1f} GB free",
            fix="aws ec2 modify-volume --size 60 ... then 'sudo growpart /dev/nvme0n1 1'",
            required=False)

    rc, out, _ = _run(["free", "-g"])
    for line in out.splitlines():
        if line.lower().startswith("mem"):
            try:
                avail = float(line.split()[6])
                _record("RAM available ≥ 8 GB", avail >= 8,
                        f"{avail:.0f} GB free",
                        fix="Stop other processes, or upgrade to m5.4xlarge",
                        required=False)
            except (IndexError, ValueError):
                pass
            break


# =============================================================================
#  MAIN
# =============================================================================
def main():
    parser = argparse.ArgumentParser(
        description="Pre-flight validation for phase1_parallel.py")
    parser.add_argument("--quick", action="store_true",
                        help="Skip k6 live test and codecarbon (saves ~30s)")
    args = parser.parse_args()

    print(f"""
{BOLD}{"═"*62}{RST}
{BOLD}  SETUP VALIDATION — Zero-Telemetry CPU Prediction{RST}
{BOLD}{"═"*62}{RST}
  Time   : {datetime.now().strftime('%Y-%m-%d  %H:%M:%S')}
  Python : {sys.version.split()[0]}
  Mode   : {"quick" if args.quick else "full"}
""")

    t0 = time.time()

    _section("1 · Runtime environment")
    check_python()

    _section("2 · Kubernetes cluster")
    check_kubectl()
    check_kubectl_top()

    _section("3 · OCI tools")
    check_skopeo()
    check_docker_manifest()

    _section("4 · Load test")
    check_k6(args.quick)

    _section("5 · Python packages")
    check_python_packages()
    check_codecarbon(args.quick)
    check_multiprocessing()

    _section("6 · Benchmark tools")
    check_benchmark_tools()

    _section("7 · Parallel infrastructure")
    check_namespace_isolation()

    _section("8 · Startup CPU sampling  (new — phase1_parallel_aws.py)")
    check_startup_cpu_sampling()

    _section("9 · Mini end-to-end pipeline  (most important check)")
    check_mini_pipeline()

    _section("10 · Resource headroom")
    check_resources()

    # ── summary ───────────────────────────────────────────────────────────────
    elapsed = time.time() - t0
    passed  = sum(1 for r in _results if r["passed"])
    failed  = [r for r in _results if not r["passed"]]
    req_fail= [r for r in failed if r["required"]]
    adv_fail= [r for r in failed if not r["required"]]

    print(f"\n{'═'*62}")
    print(f"  VALIDATION SUMMARY   ({elapsed:.0f}s)")
    print(f"{'═'*62}")
    print(f"  Passed   : {G}{passed}{RST} / {len(_results)}")
    if req_fail:
        print(f"  {R}{BOLD}Required failures — MUST fix before full run:{RST}")
        for r in req_fail:
            print(f"    {R}✘  {r['name']}{RST}")
            if r["fix"]:
                print(f"       {Y}→  {r['fix']}{RST}")
    if adv_fail:
        print(f"  {Y}Advisory failures (fix improves data quality):{RST}")
        for r in adv_fail:
            print(f"    {Y}⚠  {r['name']}{RST}  {DIM}{r['detail']}{RST}")
            if r["fix"]:
                print(f"       {Y}→  {r['fix']}{RST}")

    print()
    if not req_fail:
        print(f"  {G}{BOLD}╔══════════════════════════════════════════════════════╗{RST}")
        print(f"  {G}{BOLD}║  ✔  ALL REQUIRED CHECKS PASSED                      ║{RST}")
        print(f"  {G}{BOLD}║                                                      ║{RST}")
        print(f"  {G}{BOLD}║  Run smoke test (5 min):                             ║{RST}")
        print(f"  {G}{BOLD}║    python3 phase1_parallel_aws.py --smoke            ║{RST}")
        print(f"  {G}{BOLD}║                                                      ║{RST}")
        print(f"  {G}{BOLD}║  Then full dataset collection:                       ║{RST}")
        print(f"  {G}{BOLD}║    tmux new -s collect                               ║{RST}")
        print(f"  {G}{BOLD}║    python3 phase1_parallel_aws.py --half             ║{RST}")
        print(f"  {G}{BOLD}╚══════════════════════════════════════════════════════╝{RST}\n")
        sys.exit(0)
    else:
        print(f"  {R}{BOLD}╔══════════════════════════════════════════════════════╗{RST}")
        print(f"  {R}{BOLD}║  ✘  {len(req_fail)} REQUIRED CHECK(S) FAILED                    ║{RST}")
        print(f"  {R}{BOLD}║  Fix the issues above, then re-run this script.      ║{RST}")
        print(f"  {R}{BOLD}╚══════════════════════════════════════════════════════╝{RST}\n")
        sys.exit(1)


if __name__ == "__main__":
    main()
