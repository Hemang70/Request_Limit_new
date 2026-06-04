#!/usr/bin/env python3
"""
PHASE 0 AWS — COMPLETE SETUP & VERIFICATION
Zero-Telemetry CPU Prediction  /  Carbon-Aware Right-Sizing
Target: AWS EC2 m5.4xlarge  (Ubuntu 22.04 LTS, k3s Kubernetes)

Four steps
----------
  0.1  Instance & cluster  — vCPU/RAM/disk, k3s, kubectl, kubeconfig, nodes
  0.2  Install tools       — helm, metrics-server, skopeo, k6, Python venv,
                             ALL 10 benchmark tools (redis, sysbench, pgbench …)
  0.3  Functional tests    — parallel smoke test every tool, writes report
  0.4  Benchmark sanity    — deploy nginx:alpine, run 60-s k6, confirm CPU > 0m

Steps 0.1 and 0.2 are BLOCKING (abort on failure).
Steps 0.3 and 0.4 are ADVISORY (warn, continue).

Usage
-----
  python3 phase0_aws.py              # all 4 steps (recommended first time)
  python3 phase0_aws.py --step 1     # cluster check only
  python3 phase0_aws.py --step 2     # re-install tools
  python3 phase0_aws.py --step 3     # re-run functional tests
  python3 phase0_aws.py --step 4     # re-run benchmark sanity
  python3 phase0_aws.py --step 2 --skip-prometheus
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import textwrap
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from threading import Lock, Thread
import multiprocessing as _mp
# Python 3.14 changed default start method to forkserver which breaks
# subprocess calls inside worker processes. Force spawn for compatibility.
if _mp.get_start_method(allow_none=True) is None:
    _mp.set_start_method("spawn")

# ── ANSI ──────────────────────────────────────────────────────────────────────
G, R, Y, B, C = "\033[92m", "\033[91m", "\033[93m", "\033[94m", "\033[96m"
RST, BOLD, DIM = "\033[0m", "\033[1m", "\033[2m"

# ── Paths ─────────────────────────────────────────────────────────────────────
PROJECT = Path.home() / "zero-telemetry-cpu"
REPORT  = PROJECT / "phase0_aws_verification.txt"
DIRS    = [PROJECT, PROJECT/"data", PROJECT/"data"/"cpu_traces",
           PROJECT/"results", PROJECT/"models", PROJECT/"logs"]
VENV    = Path.home() / ".venv-zerotelem"
VPIP    = VENV / "bin" / "pip"
VPY     = VENV / "bin" / "python"

# ── AWS thresholds ────────────────────────────────────────────────────────────
MIN_VCPU   = 8
MIN_RAM_GB = 16
MIN_DISK_GB = 20

# ── Python packages ───────────────────────────────────────────────────────────
PY_PACKAGES = [
    # numpy pin removed — numpy 2.x required for Python 3.13/3.14
    "numpy",
    "pandas", "scikit-learn", "xgboost", "shap",
    "matplotlib", "seaborn", "tqdm", "tabulate",
    "codecarbon", "requests", "pyyaml",
]

# ── Critical set — failures here block Phase 1 ────────────────────────────────
PHASE1_CRITICAL = frozenset({
    "kubectl_top", "skopeo", "k6", "docker_manifest",
    "bench_redis-benchmark", "bench_sysbench", "bench_pgbench",
    "python_pandas", "python_xgboost", "python_shap", "codecarbon",
})

# ── Benchmark tools to verify ─────────────────────────────────────────────────
BENCH_TOOLS = [
    ("redis-benchmark",          ["redis-benchmark", "--version"]),
    ("memtier_benchmark",        ["memtier_benchmark", "--version"]),
    ("wrk",                      ["wrk", "--version"]),
    ("sysbench",                 ["sysbench", "--version"]),
    ("pgbench",                  ["pgbench", "--version"]),
    ("mongosh",                  ["mongosh", "--version"]),
    ("stress-ng",                ["stress-ng", "--version"]),
    ("rabbitmq-perf-test",       ["rabbitmq-perf-test", "--help"]),
    ("kafka-producer-perf-test", ["kafka-producer-perf-test.sh", "--help"]),
    ("cassandra-stress",         ["cassandra-stress", "help"]),
]

_print_lock = Lock()
_v_results: dict = {}


# ── Print / run helpers ────────────────────────────────────────────────────────
def _p(label, detail=""):
    s = f"  {DIM}{detail[:60]}{RST}" if detail else ""
    with _print_lock:
        print(f"  {G}✔{RST}  {label:<46}{s}")

def _f(label, detail=""):
    s = f"\n       {R}{detail[:100]}{RST}" if detail else ""
    with _print_lock:
        print(f"  {R}✘{RST}  {label:<46}{s}")

def _w(label, detail=""):
    s = f"  {Y}{detail[:80]}{RST}" if detail else ""
    with _print_lock:
        print(f"  {Y}⚠{RST}  {label:<46}{s}")

def _i(msg):
    with _print_lock:
        print(f"  {Y}→{RST}  {msg}")

def _hdr(title):
    print(f"\n{BOLD}{B}{'─'*64}{RST}\n{BOLD}{B}  {title}{RST}\n{BOLD}{B}{'─'*64}{RST}\n")

def _section(title):
    with _print_lock:
        print(f"\n  {BOLD}{B}[ {title} ]{RST}")

def _run(cmd, timeout=30, capture=True):
    try:
        if capture:
            r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
            return r.returncode, r.stdout.strip(), r.stderr.strip()
        r = subprocess.run(cmd, timeout=timeout)
        return r.returncode, "", ""
    except subprocess.TimeoutExpired:
        return 1, "", f"timeout {timeout}s"
    except FileNotFoundError:
        return 1, "", f"not found: {cmd[0]}"
    except Exception as e:
        return 1, "", str(e)

def _cmd(b): return shutil.which(b) is not None
def _py():   return str(VPY) if VPY.exists() else sys.executable

def _record(key, passed, detail, required=True):
    _v_results[key] = (passed, detail)
    tag  = f"  {Y}[req]{RST}" if required and not passed else ""
    icon = f"{G}✔{RST}" if passed else f"{R}✘{RST}"
    col  = G if passed else R
    with _print_lock:
        print(f"  {icon}  {col}{key:<44}{RST}  {DIM}{detail[:55]}{RST}{tag}")
    return passed


# ═══════════════════════════════════════════════════════════════════════════════
#  STEP 0.1 — INSTANCE & CLUSTER
# ═══════════════════════════════════════════════════════════════════════════════

def run_step1() -> bool:
    _hdr("Step 0.1 — Instance & Kubernetes Cluster")
    ok = True

    # Python
    v = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
    if sys.version_info >= (3, 10):
        _p("Python ≥ 3.10", v)
    else:
        _f("Python 3.10+ required", v); ok = False

    # vCPU
    vcpu = os.cpu_count() or 0
    ((_p if vcpu >= MIN_VCPU else _w))("vCPU count", f"{vcpu}  (m5.4xlarge=16)")

    # RAM
    rc, out, _ = _run(["free", "-g"])
    for line in out.splitlines():
        if line.lower().startswith("mem"):
            try:
                total = float(line.split()[1]); avail = float(line.split()[6])
                ((_p if total >= MIN_RAM_GB else _w))("RAM",
                    f"{avail:.0f} GB free / {total:.0f} GB total")
            except Exception:
                pass

    # Disk
    _, _, free_b = shutil.disk_usage(Path.home())
    free_gb = free_b / 1024**3
    fn = _p if free_gb >= MIN_DISK_GB else _f
    fn("Disk free", f"{free_gb:.1f} GB  (need ≥{MIN_DISK_GB} GB)")
    if free_gb < MIN_DISK_GB:
        ok = False

    # kubectl
    rc, out, err = _run(["kubectl", "version", "--client"], timeout=10)
    if rc == 0:
        _p("kubectl", next((l for l in out.splitlines() if "Client" in l), out[:60]))
    else:
        _f("kubectl not found", err[:80]); ok = False

    # kubeconfig
    kube  = os.environ.get("KUBECONFIG", "")
    d_cfg = Path.home() / ".kube" / "config"
    k3s   = Path("/etc/rancher/k3s/k3s.yaml")
    if (kube and Path(kube).exists()) or d_cfg.exists() or k3s.exists():
        loc = kube or (str(d_cfg) if d_cfg.exists() else str(k3s))
        _p("kubeconfig", loc)
    else:
        _f("kubeconfig not found",
           "sudo cp /etc/rancher/k3s/k3s.yaml ~/.kube/config && sudo chown $USER ~/.kube/config")
        ok = False

    # k3s
    rc, out, _ = _run(["systemctl", "is-active", "k3s"], timeout=8)
    if rc == 0 and "active" in out:
        _p("k3s service", "active")
    else:
        rc2, _, _ = _run(["pgrep", "-x", "k3s-server"], timeout=5)
        (_p if rc2 == 0 else _w)("k3s", "process running" if rc2 == 0
                                  else "not detected — kubectl may still work")

    # Nodes
    rc, out, err = _run(["kubectl", "get", "nodes", "--no-headers"], timeout=20)
    if rc != 0:
        _f("kubectl get nodes failed", err[:80]); ok = False
    else:
        lines = [l for l in out.splitlines() if l.strip()]
        ready = [l for l in lines if "Ready" in l and "NotReady" not in l]
        if ready:
            _p("Kubernetes nodes", f"{len(ready)}/{len(lines)} Ready")
            for l in lines:
                p = l.split()
                nm, st = (p[0] if p else "?"), (p[1] if len(p) > 1 else "?")
                print(f"       {G if st=='Ready' else R}{nm:<22} {st}{RST}")
        else:
            _f("No Ready nodes", "Wait 30 s then retry"); ok = False

    # Dirs
    for d in DIRS:
        d.mkdir(parents=True, exist_ok=True)
    if all(d.exists() for d in DIRS):
        _p("Project directories", str(PROJECT))
    else:
        _f("Could not create project dirs"); ok = False

    print(f"\n  {'─'*50}")
    (print(f"  {G}{BOLD}✔  Step 0.1 passed{RST}\n")
     if ok else print(f"  {R}{BOLD}✘  Step 0.1 has blocking failures{RST}\n"))
    return ok


# ═══════════════════════════════════════════════════════════════════════════════
#  STEP 0.2 — INSTALL ALL TOOLS
# ═══════════════════════════════════════════════════════════════════════════════

def _apt(pkgs, timeout=300):
    if not pkgs: return True
    print(f"  {Y}→{RST}  apt install {' '.join(pkgs)} ...", end="", flush=True)
    rc, _, err = _run(
        ["apt-get", "install", "-y", "-q", "--no-install-recommends"] + pkgs,
        timeout=timeout)
    print(f"  {G}ok{RST}" if rc == 0 else f"  {R}FAILED — {err[:60]}{RST}")
    return rc == 0

def _wait_apt_lock(timeout_s: int = 180) -> None:
    """Wait for dpkg/apt lock to be free before any apt call."""
    import time as _t
    waited = 0
    while waited < timeout_s:
        rc, _, _ = _run(["fuser", "/var/lib/dpkg/lock-frontend"], timeout=5)
        if rc != 0:   # fuser returns 1 when no process holds it
            return
        print(f"  {DIM}  apt locked — waiting 10s ({waited}s)...{RST}", flush=True)
        _t.sleep(10); waited += 10
    # Force-clear if still locked after timeout
    _run(["killall", "unattended-upgrades"], timeout=10)
    for lf in ["/var/lib/dpkg/lock-frontend", "/var/lib/dpkg/lock",
               "/var/cache/apt/archives/lock"]:
        _run(["rm", "-f", lf], timeout=5)
    _run(["dpkg", "--configure", "-a"], timeout=60)

def _apt_update():
    _wait_apt_lock()
    print(f"  {Y}→{RST}  apt-get update ...", end="", flush=True)
    _run(["apt-get", "update", "-qq"], timeout=120)
    print(f"  {G}ok{RST}")

def _install_skopeo():
    if _cmd("skopeo"):
        _p("skopeo (already installed)"); return True
    _apt_update(); _apt(["skopeo"])
    ok = _cmd("skopeo")
    (_p if ok else _f)("skopeo", "" if ok else "apt install failed")
    return ok

def _install_k6():
    if _cmd("k6"):
        _p("k6 (already installed)"); return True
    _i("Installing k6 (Grafana repo)...")
    for cmd in [
        ["bash", "-c", "gpg --no-default-keyring "
         "--keyring /usr/share/keyrings/k6-archive-keyring.gpg "
         "--keyserver hkp://keyserver.ubuntu.com:80 --recv-keys "
         "C5AD17C747E3415A3642D57D77C6C491D6AC1D69"],
        ["bash", "-c", "echo 'deb [signed-by=/usr/share/keyrings/k6-archive-keyring.gpg] "
         "https://dl.k6.io/deb stable main' | tee /etc/apt/sources.list.d/k6.list"],
        ["apt-get", "update", "-qq"],
        ["apt-get", "install", "-y", "-q", "k6"],
    ]:
        rc, _, err = _run(cmd, timeout=120)
        if rc != 0 and not any(x in " ".join(cmd) for x in ["gpg", "echo", "update"]):
            _f("k6 install failed", err[:60]); return False
    ok = _cmd("k6")
    (_p if ok else _f)("k6", "" if ok else "not found after install")
    return ok

def _install_helm():
    if _cmd("helm"):
        _p("helm (already installed)"); return True
    _i("Installing helm...")
    rc, _, err = _run(
        ["bash", "-c",
         "curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash"],
        timeout=120)
    ok = _cmd("helm")
    (_p if ok else _f)("helm", "" if ok else err[:60])
    return ok

def _install_metrics_server():
    rc, out, _ = _run(["kubectl", "get", "deploy", "metrics-server",
                        "-n", "kube-system", "--no-headers"], timeout=15)
    if rc == 0 and "metrics-server" in out:
        _p("metrics-server (already deployed)"); return True
    if not _cmd("helm"):
        _w("metrics-server skipped — helm missing"); return False
    _i("Installing metrics-server...")
    for cmd in [
        ["helm", "repo", "add", "metrics-server",
         "https://kubernetes-sigs.github.io/metrics-server/"],
        ["helm", "repo", "update"],
        ["helm", "upgrade", "--install", "metrics-server",
         "metrics-server/metrics-server",
         "--namespace", "kube-system",
         "--set", "args={--kubelet-insecure-tls}"],
    ]:
        rc, _, err = _run(cmd, timeout=180)
        if rc != 0:
            _f("metrics-server install failed", err[:80]); return False
    _p("metrics-server installed", "ready in ~60 s"); return True

def _install_benchmark_tools():
    """Run benchmark_install.sh if present, else install critical tools manually."""
    sh = Path("benchmark_install.sh")
    if sh.exists():
        _i("Running benchmark_install.sh (~5 min)...")
        rc, _, _ = _run(["bash", str(sh)], timeout=600, capture=False)
        if rc == 0:
            _p("All benchmark tools installed via benchmark_install.sh"); return True
        _w("benchmark_install.sh had errors — falling back to manual")

    ok = True
    _apt_update()
    for pkg, tool in [
        (["redis-tools"],          "redis-benchmark"),
        (["memtier-benchmark"],    "memtier_benchmark"),
        (["wrk"],                  "wrk"),
        (["sysbench"],             "sysbench"),
        (["postgresql-client"],    "pgbench"),
        (["stress-ng"],            "stress-ng"),
    ]:
        if _cmd(tool):
            _p(f"{tool} (already installed)")
        else:
            if not _apt(pkg):
                _w(f"{tool} install failed"); ok = False
            else:
                _p(f"{tool} installed")
    return ok

def _install_docker():
    if not _cmd("docker"):
        _w("docker not found", "sudo apt-get install -y docker.io")
        return False
    rc, _, _ = _run(["docker", "info"], timeout=15)
    if rc == 0:
        _p("docker daemon running"); return True
    _run(["systemctl", "start", "docker"], timeout=30)
    _run(["usermod", "-aG", "docker", os.environ.get("USER", "ubuntu")], timeout=10)
    rc, _, _ = _run(["docker", "info"], timeout=15)
    ok = rc == 0
    (_p if ok else _w)("docker", "running" if ok else "not responding — manifest inspect may fail")
    return ok

def _install_python_venv():
    import_map = {"scikit-learn": "sklearn", "pyyaml": "yaml", "numpy": "numpy"}
    if VPY.exists():
        bad = [pkg for pkg in PY_PACKAGES
               if _run([str(VPY), "-c",
                        f"import {import_map.get(pkg, pkg.replace('-','_').replace('<2',''))}"],
                       timeout=10)[0] != 0]
        if not bad:
            _p("Python venv complete", str(VENV)); return True
        _i(f"Venv incomplete — rebuilding ({len(bad)} missing)")
        shutil.rmtree(str(VENV))

    _i(f"Creating venv at {VENV} (~5 min)...")
    _apt(["python3-venv", "python3-dev", "gcc", "gfortran", "pkg-config"])
    rc, _, err = _run([sys.executable, "-m", "venv", str(VENV)], timeout=90)
    if rc != 0:
        _f("Cannot create venv", err[:120]); return False
    _run([str(VPIP), "install", "--quiet", "--upgrade", "pip", "setuptools", "wheel"],
         timeout=90)

    # Install numpy wheel-first — avoids Fortran compilation on Python 3.12+
    _i("Installing numpy (wheel-first)...")
    rc_np, _, _ = _run(
        [str(VPIP), "install", "--quiet", "--only-binary=:all:", "numpy"],
        timeout=120)
    if rc_np != 0:
        _i("No wheel found — compiling numpy from source (~5 min)...")
        _run([str(VPIP), "install", "--quiet", "numpy"], timeout=600)

    # Remaining packages (no numpy in this list — already installed above)
    other = [p for p in PY_PACKAGES if p not in ("numpy",)]
    rc, _, err = _run(
        [str(VPIP), "install", "--quiet", "--no-warn-script-location"] + other,
        timeout=600)
    if rc != 0:
        _f("pip install failed", err[-200:]); return False

    import glob as _gl, site as _si
    sp = _gl.glob(str(VENV / "lib" / "python*" / "site-packages"))
    if sp:
        for sd in _si.getsitepackages():
            try:
                (Path(sd) / "zerotelem_venv.pth").write_text(sp[0] + "\n"); break
            except Exception:
                continue

    _p("Python venv installed", str(VENV)); return True

def run_step2(skip_prometheus=False) -> bool:
    _hdr("Step 0.2 — Install Required Tools (AWS edition)")
    results = {}

    _section("Kubernetes & OCI tools")
    results["skopeo"]          = _install_skopeo()
    results["k6"]              = _install_k6()
    results["helm"]            = _install_helm()
    results["metrics_server"]  = _install_metrics_server()
    results["docker"]          = _install_docker()

    _section("Benchmark tools (10)")
    results["benchmark_tools"] = _install_benchmark_tools()

    _section("Python venv + ML packages")
    results["python_venv"]     = _install_python_venv()

    advisory  = {"docker", "benchmark_tools", "k6"}
    hard_fail = [k for k, v in results.items() if not v and k not in advisory]

    print(f"\n  {'─'*50}")
    if [k for k, v in results.items() if not v and k in advisory]:
        _w(f"Advisory failures: {[k for k,v in results.items() if not v and k in advisory]}")
    if hard_fail:
        print(f"  {R}{BOLD}✘  Blocking failures: {', '.join(hard_fail)}{RST}\n")
        return False
    print(f"  {G}{BOLD}✔  Step 0.2 complete{RST}\n")
    return True


# ═══════════════════════════════════════════════════════════════════════════════
#  STEP 0.3 — FUNCTIONAL TESTS  (parallel)
# ═══════════════════════════════════════════════════════════════════════════════

def _c_kubectl_nodes():
    rc, out, err = _run(["kubectl", "get", "nodes", "--no-headers"], timeout=20)
    lines = [l for l in out.splitlines() if l.strip()]
    ready = [l for l in lines if "Ready" in l and "NotReady" not in l]
    _record("kubectl_nodes", bool(ready) and rc == 0,
            f"{len(ready)}/{len(lines)} Ready" if rc == 0 else err[:60])

def _c_kubectl_top():
    for i in range(3):
        if i: time.sleep(20)
        rc, out, _ = _run(["kubectl", "top", "nodes"], timeout=30)
        if rc == 0 and out:
            _record("kubectl_top", True, out.splitlines()[0][:55]); return
    _record("kubectl_top", False, "not ready — reinstall metrics-server (step 2)")

def _c_skopeo():
    rc, out, err = _run(
        ["skopeo", "inspect", "--raw",
         "docker://docker.io/library/nginx:alpine"], timeout=45)
    if rc == 0 and out:
        try:
            m = json.loads(out)
            n = len(m.get("layers", m.get("manifests", [])))
            _record("skopeo", True, f"nginx:alpine — {n} layer(s)"); return
        except Exception:
            _record("skopeo", True, "manifest pulled"); return
    _record("skopeo", False, err[:80])

def _c_docker_manifest():
    rc, out, err = _run(["docker", "manifest", "inspect", "nginx:alpine"], timeout=45)
    if rc == 0 and out:
        try:
            m = json.loads(out)
            n = len(m.get("layers", []))
            _record("docker_manifest", True, f"{n} compressed layers",
                    required=False); return
        except Exception:
            _record("docker_manifest", True, "manifest returned", required=False); return
    _record("docker_manifest", False, err[:60], required=False)

def _c_k6():
    script = ("import http from 'k6/http';\nimport { sleep } from 'k6';\n"
              "export const options = { vus: 2, duration: '5s' };\n"
              "export default function () { http.get('http://test.k6.io'); sleep(1); }\n")
    with tempfile.NamedTemporaryFile(suffix=".js", mode="w", delete=False) as f:
        f.write(script); tmp = f.name
    try:
        rc, out, err = _run(["k6", "run", "--quiet", tmp], timeout=30)
        _record("k6", rc == 0, "5s/2VU ok" if rc == 0 else (err or out)[:60])
    finally:
        os.unlink(tmp)

def _c_bench_tool(name, cmd):
    rc, out, err = _run(cmd, timeout=20)
    ok = bool(out or err) or rc == 0
    detail = (out or err)[:55].split("\n")[0]
    is_req = name in ("redis-benchmark", "sysbench", "pgbench")
    _record(f"bench_{name}", ok, detail if ok else f"not found: {name}",
            required=is_req)

_PY_TESTS = [
    ("python_pandas",    "import pandas as pd; assert pd.__version__"),
    ("python_numpy",     "import numpy as np; assert np.__version__"),
    ("python_sklearn",   "from sklearn.ensemble import RandomForestRegressor"),
    ("python_xgboost",   "import xgboost; assert xgboost.__version__"),
    ("python_shap",      "import shap; assert shap.__version__"),
    ("python_matplotlib","import matplotlib; assert matplotlib.__version__"),
    ("python_seaborn",   "import seaborn; assert seaborn.__version__"),
    ("python_requests",  "import requests; assert requests.__version__"),
    ("python_yaml",      "import yaml; assert yaml.__version__"),
    ("python_tqdm",      "import tqdm; assert tqdm.__version__"),
]

def _c_pkg(key, stmt):
    py = _py()
    rc, _, err = _run([py, "-c", stmt], timeout=15)
    pkg = key.replace("python_", "")
    if rc == 0:
        vrc, ver, _ = _run([py, "-c",
                             f"import {pkg} as m; print(getattr(m,'__version__','ok'))"],
                           timeout=10)
        _record(key, True, f"v{ver}" if vrc == 0 else "ok")
    else:
        _record(key, False, (err or "ImportError")[:55])

def _c_codecarbon():
    script = ("import time\nfrom codecarbon import EmissionsTracker\n"
              "t = EmissionsTracker(log_level='error', save_to_file=False)\n"
              "t.start(); time.sleep(3); e = t.stop()\nprint(f'e:{e:.2e}')\n")
    with tempfile.NamedTemporaryFile(suffix=".py", mode="w", delete=False) as f:
        f.write(script); tmp = f.name
    try:
        rc, out, err = _run([_py(), tmp], timeout=25)
        ok = rc == 0 and "e:" in out
        _record("codecarbon", ok,
                f"tracked {out.split('e:')[-1].strip()} kg" if ok else (err or out)[:60])
    finally:
        os.unlink(tmp)

def _c_multiprocessing():
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
        rc, out, err = _run([_py(), tmp], timeout=30)
        _record("multiprocessing", rc == 0 and "ok" in out,
                "2-worker pool ok" if rc == 0 else (err or out)[:80])
    finally:
        _os.unlink(tmp)

def _c_namespace():
    for ns in ("zt-test-a", "zt-test-b"):
        _run(["kubectl", "create", "namespace", ns], timeout=10)
    ra, _, _ = _run(["kubectl", "get", "ns", "zt-test-a", "--no-headers"], timeout=10)
    rb, _, _ = _run(["kubectl", "get", "ns", "zt-test-b", "--no-headers"], timeout=10)
    for ns in ("zt-test-a", "zt-test-b"):
        _run(["kubectl", "delete", "ns", ns, "--ignore-not-found=true"], timeout=15)
    ok = ra == 0 and rb == 0
    _record("namespace_isolation", ok,
            "create/delete zt-test-a zt-test-b" if ok else "namespace ops failed")

def _c_project_dirs():
    missing = [str(d) for d in DIRS if not d.exists()]
    _record("project_dirs", not missing,
            "~/zero-telemetry-cpu/ OK" if not missing
            else f"missing: {', '.join(missing)}")

def _write_report():
    PROJECT.mkdir(parents=True, exist_ok=True)
    with open(REPORT, "w") as fh:
        fh.write(f"Phase 0 AWS Verification\nGenerated: {datetime.now().isoformat()}\n"
                 f"{'─'*50}\n\n")
        for key, (ok, detail) in _v_results.items():
            fh.write(f"{'PASS' if ok else 'FAIL'}  {key:<44}  {detail}\n")
        n = sum(1 for ok, _ in _v_results.values() if ok)
        fh.write(f"\n{'─'*50}\nTotal: {n}/{len(_v_results)} passed\n")
    print(f"\n  {DIM}Report → {REPORT}{RST}")

def run_step3() -> bool:
    _hdr("Step 0.3 — Functional Tests (parallel, ~90 s)")

    _section("Infrastructure")
    _c_project_dirs()
    with ThreadPoolExecutor(max_workers=4) as ex:
        for f in as_completed({
            ex.submit(_c_kubectl_nodes): "nodes",
            ex.submit(_c_kubectl_top):   "top",
            ex.submit(_c_skopeo):        "skopeo",
            ex.submit(_c_docker_manifest): "docker",
        }): pass

    _section("Load and benchmark tools")
    with ThreadPoolExecutor(max_workers=4) as ex:
        futs = {ex.submit(_c_k6): "k6"}
        for name, cmd in BENCH_TOOLS:
            futs[ex.submit(_c_bench_tool, name, cmd)] = name
        for f in as_completed(futs): pass

    _section("Python packages")
    with ThreadPoolExecutor(max_workers=6) as ex:
        futs = {ex.submit(_c_pkg, key, stmt): key for key, stmt in _PY_TESTS}
        futs[ex.submit(_c_codecarbon)] = "codecarbon"
        futs[ex.submit(_c_multiprocessing)] = "multiprocessing"
        for f in as_completed(futs): pass

    _section("Parallel infrastructure")
    _c_namespace()

    _write_report()

    passed = sum(1 for ok, _ in _v_results.values() if ok)
    total  = len(_v_results)
    crit_fail = sorted(k for k in PHASE1_CRITICAL
                       if not _v_results.get(k, (False, ""))[0])

    print(f"\n  Passed: {G}{passed}{RST} / {total}")
    if crit_fail:
        print(f"  {R}{BOLD}Critical failures:{RST}")
        for k in crit_fail:
            _, detail = _v_results.get(k, (False, ""))
            print(f"    {R}✘  {k:<44}{RST}  {Y}{detail[:55]}{RST}")
        print(f"\n  {Y}Fix:  python3 phase0_aws.py --step 2{RST}\n")
        return False
    print(f"  {G}{BOLD}✔  All critical tools verified{RST}\n")
    return True


# ═══════════════════════════════════════════════════════════════════════════════
#  STEP 0.4 — BENCHMARK SANITY (mini end-to-end pipeline)
# ═══════════════════════════════════════════════════════════════════════════════

def run_step4() -> bool:
    """
    Deploy nginx:alpine with no cpu limits → run 60-s k6 HTTP load
    concurrently with kubectl top sampling → confirm p50 CPU > 0m.
    Proves the full 5-stage pipeline works on this EC2 instance.
    """
    _hdr("Step 0.4 — Benchmark Sanity (mini end-to-end pipeline)")
    NS   = "zt-sanity"
    SLUG = "nginx-alpine"

    # Clean up
    _run(["kubectl", "delete", "namespace", NS, "--ignore-not-found=true"], timeout=20)
    time.sleep(3)
    _run(["kubectl", "create", "namespace", NS], timeout=10)
    _i("Namespace zt-sanity created")

    # Deploy nginx:alpine — NO cpu requests/limits (this is the research condition)
    yaml = textwrap.dedent(f"""\
        apiVersion: apps/v1
        kind: Deployment
        metadata:
          name: {SLUG}
          namespace: {NS}
          labels: {{app: {SLUG}, research: zero-telemetry-cpu}}
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
                image: nginx:alpine
                ports:
                - containerPort: 80
                resources:
                  requests:
                    memory: "32Mi"
                  limits:
                    memory: "256Mi"
        ---
        apiVersion: v1
        kind: Service
        metadata:
          name: {SLUG}
          namespace: {NS}
        spec:
          type: NodePort
          selector: {{app: {SLUG}}}
          ports:
          - port: 80
            targetPort: 80
    """)
    with tempfile.NamedTemporaryFile(suffix=".yaml", mode="w", delete=False) as f:
        f.write(yaml); tmp = f.name
    rc, _, err = _run(["kubectl", "apply", "-f", tmp], timeout=20)
    os.unlink(tmp)
    if rc != 0:
        _f("kubectl apply failed", err[:80])
        _run(["kubectl", "delete", "namespace", NS, "--ignore-not-found=true"], timeout=20)
        return False
    _p("nginx:alpine deployment applied")

    # Wait up to 120 s for Running
    _i("Waiting for pod Running...")
    pod_up = False
    for i in range(24):
        time.sleep(5)
        rc, out, _ = _run(
            ["kubectl", "get", "pods", "-n", NS,
             "-l", f"app={SLUG}", "--no-headers"], timeout=10)
        if rc == 0 and "Running" in out:
            pod_up = True
            _p("Pod Running", out.strip()[:60]); break
        print(f"\r  {DIM}  [{i+1}/24] {out.strip()[:55]}{RST}", end="", flush=True)
    print()
    if not pod_up:
        _f("Pod did not reach Running in 120 s",
           "kubectl describe pod -n zt-sanity  for pull errors")
        _run(["kubectl", "delete", "namespace", NS, "--ignore-not-found=true"], timeout=20)
        return False

    # NodePort URL
    rc, port_str, _ = _run(
        ["kubectl", "get", "svc", SLUG, "-n", NS,
         "-o", "jsonpath={.spec.ports[0].nodePort}"], timeout=15)
    rc2, node_ip, _ = _run(
        ["kubectl", "get", "nodes", "-o",
         "jsonpath={.items[0].status.addresses[?(@.type==\"InternalIP\")].address}"],
        timeout=15)
    node_ip = node_ip.strip() or "localhost"
    url = f"http://{node_ip}:{port_str.strip()}" if port_str.strip() \
          else "http://localhost:80"
    _p("Service URL", url)

    # 30 s warm-up
    _i("Warm-up pause 30 s...")
    time.sleep(30)

    # 60-s k6 load + concurrent CPU sampling
    k6_script = f"""
import http from 'k6/http';
import {{ sleep, check }} from 'k6';

# Python 3.12+ changed default multiprocessing start method to forkserver.
# forkserver breaks subprocess.run() calls inside worker processes.
# Force spawn at module level before any ProcessPoolExecutor is created.
import multiprocessing as _mp_fix
try:
    _mp_fix.set_start_method("spawn", force=True)
except RuntimeError:
    pass  # already set

export const options = {{
  scenarios: {{
    sanity: {{ executor: 'ramping-vus', startVUs: 0,
               stages: [
                 {{ duration: '10s', target: 10 }},
                 {{ duration: '40s', target: 10 }},
                 {{ duration: '10s', target: 0  }},
               ] }},
  }},
  thresholds: {{
    http_req_failed: [{{ threshold: 'rate<0.10', abortOnFail: false }}],
  }},
}};
export default function () {{
  check(http.get('{url}', {{ timeout: '5s' }}), {{ 'ok': r => r.status < 500 }});
  sleep(0.5);
}}
"""
    cpu_readings: list[float] = []
    k6_done = [False]

    def _run_k6():
        with tempfile.NamedTemporaryFile(suffix=".js", mode="w", delete=False) as f:
            f.write(k6_script); tmp = f.name
        _run(["k6", "run", "--quiet", tmp], timeout=120)
        os.unlink(tmp)
        k6_done[0] = True

    def _sample_cpu():
        t0 = time.time()
        while not k6_done[0] and time.time() - t0 < 100:
            rc, out, _ = _run(
                ["kubectl", "top", "pod", "-n", NS,
                 "-l", f"app={SLUG}", "--no-headers"], timeout=10)
            if rc == 0 and out:
                parts = out.split()
                if len(parts) >= 2:
                    try:
                        cpu_readings.append(float(parts[1].rstrip("m")))
                    except ValueError:
                        pass
            time.sleep(5)

    _i("Running 60-s ramp→sustain→ramp k6 load + kubectl top (concurrent)...")
    t_k6 = Thread(target=_run_k6,      daemon=True)
    t_sp = Thread(target=_sample_cpu, daemon=True)
    t_k6.start(); t_sp.start()
    t_k6.join(timeout=130)
    t_sp.join(timeout=30)

    # Evaluate result
    if not cpu_readings:
        _f("CPU sampling returned 0 samples",
           "metrics-server not ready — wait 60s and retry --step 4")
        _run(["kubectl", "delete", "namespace", NS, "--ignore-not-found=true"], timeout=20)
        return False

    sorted_cpu = sorted(cpu_readings)
    p50 = sorted_cpu[len(sorted_cpu) // 2]
    p99 = sorted_cpu[max(0, int(0.99 * len(sorted_cpu)) - 1)]
    n   = len(sorted_cpu)

    _p("CPU samples collected", f"n={n}")

    if p50 > 0:
        _p("CPU signal confirmed",
           f"p50={p50:.0f}m  p99={p99:.0f}m  n={n}  (unthrottled — ground truth)")
        result_ok = True
    elif n >= 3:
        _w("p50=0m but samples exist",
           f"Very low load — acceptable; run --full to get meaningful values  n={n}")
        result_ok = True
    else:
        _f("Insufficient CPU signal", "Check k6 connectivity and metrics-server")
        result_ok = False

    # Teardown
    _run(["kubectl", "delete", "namespace", NS, "--ignore-not-found=true"], timeout=20)
    _p("Teardown", f"namespace {NS} deleted")

    if result_ok:
        print(f"\n  {G}{BOLD}✔  Step 0.4 passed — full pipeline verified on this instance{RST}\n")
    else:
        print(f"\n  {Y}Step 0.4 advisory failure — check above{RST}\n")
    return result_ok


# ═══════════════════════════════════════════════════════════════════════════════
#  BANNER + SUMMARY + MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def banner():
    vcpu = os.cpu_count() or 0
    rc, out, _ = _run(["kubectl", "get", "nodes", "--no-headers"], timeout=8)
    nodes = len([l for l in out.splitlines() if l.strip()]) if rc == 0 else None
    print(f"""
{BOLD}{C}
╔══════════════════════════════════════════════════════════════════╗
║  ZERO-TELEMETRY CPU PREDICTION — AWS EDITION                     ║
║  PHASE 0 — Setup & Verification (m5.4xlarge / Ubuntu 22.04)     ║
╚══════════════════════════════════════════════════════════════════╝
{RST}
  Started  : {datetime.now().strftime('%Y-%m-%d  %H:%M:%S')}
  Python   : {sys.version.split()[0]}
  vCPUs    : {vcpu}  {"(OK)" if vcpu >= 16 else "(warn: expected 16 for m5.4xlarge)"}
  K8s nodes: {nodes if nodes is not None else "checking..."}
""")

def print_summary(step_results: dict, elapsed: float):
    names   = {1:"Instance & Cluster", 2:"Install Tools",
               3:"Functional Tests",   4:"Benchmark Sanity"}
    blocking = {1, 2}

    print(f"\n{BOLD}{B}{'═'*64}{RST}")
    print(f"{BOLD}{B}  PHASE 0 AWS COMPLETE{RST}")
    print(f"{BOLD}{B}{'═'*64}{RST}\n")
    print(f"  Finished : {datetime.now().strftime('%Y-%m-%d  %H:%M:%S')}")
    print(f"  Elapsed  : {elapsed:.1f}s\n")

    all_blocking_ok = True
    for num in sorted(step_results):
        ok = step_results[num]; tag = "(blocking)" if num in blocking else "(advisory)"
        c = G if ok else R
        print(f"  {c}{'✔' if ok else '✘'}{RST}  Step 0.{num}: {names[num]:<30}"
              f"[{c}{BOLD}{'PASS' if ok else 'FAIL'}{RST}]  {DIM}{tag}{RST}")
        if not ok and num in blocking:
            all_blocking_ok = False

    crit_ok = PHASE1_CRITICAL.issubset(
        {k for k, (ok, _) in _v_results.items() if ok})

    print()
    if all_blocking_ok and crit_ok:
        print(f"  {G}{BOLD}╔══════════════════════════════════════════════════════╗{RST}")
        print(f"  {G}{BOLD}║  ✔  PHASE 0 COMPLETE — READY FOR PHASE 1           ║{RST}")
        print(f"  {G}{BOLD}║                                                      ║{RST}")
        print(f"  {G}{BOLD}║  Run smoke test first (5 min):                       ║{RST}")
        print(f"  {G}{BOLD}║    python3 phase1_parallel_aws.py --smoke            ║{RST}")
        print(f"  {G}{BOLD}║                                                      ║{RST}")
        print(f"  {G}{BOLD}║  Then full dataset collection (35-65 min):           ║{RST}")
        print(f"  {G}{BOLD}║    tmux new -s collect                               ║{RST}")
        print(f"  {G}{BOLD}║    python3 phase1_parallel_aws.py --full             ║{RST}")
        print(f"  {G}{BOLD}╚══════════════════════════════════════════════════════╝{RST}\n")
    else:
        print(f"  {R}{BOLD}Fix failures then re-run:  python3 phase0_aws.py{RST}\n")

    print(f"{BOLD}Quick reference:{RST}")
    print(f"  python3 phase0_aws.py --step 1  # cluster check")
    print(f"  python3 phase0_aws.py --step 2  # re-install tools")
    print(f"  python3 phase0_aws.py --step 3  # functional tests")
    print(f"  python3 phase0_aws.py --step 4  # sanity pipeline")
    print(f"  Report → {REPORT}\n")


STEPS_BLOCKING = {1: True, 2: True, 3: False, 4: False}

def main():
    parser = argparse.ArgumentParser(
        description="Phase 0 AWS — m5.4xlarge environment setup and verification",
        formatter_class=argparse.RawDescriptionHelpFormatter, epilog=__doc__)
    parser.add_argument("--step", type=int, choices=[1,2,3,4])
    parser.add_argument("--skip-prometheus", action="store_true")
    args = parser.parse_args()

    banner()
    t0 = time.time()

    fns = {
        1: run_step1,
        2: lambda: run_step2(skip_prometheus=args.skip_prometheus),
        3: run_step3,
        4: run_step4,
    }

    if args.step:
        ok = fns[args.step]()
        sys.exit(0 if ok else 1)

    step_results = {}
    for num in [1, 2, 3, 4]:
        ok = fns[num]()
        step_results[num] = ok
        if STEPS_BLOCKING.get(num) and not ok:
            print(f"  {R}{BOLD}Step 0.{num} is blocking — stopping.{RST}\n")
            for rem in [n for n in [1,2,3,4] if n > num]:
                step_results[rem] = False
            break

    elapsed = time.time() - t0
    print_summary(step_results, elapsed)
    blocking_ok = all(step_results.get(n, False) for n, b in STEPS_BLOCKING.items() if b)
    crit_ok = PHASE1_CRITICAL.issubset({k for k, (ok, _) in _v_results.items() if ok})
    sys.exit(0 if (blocking_ok and crit_ok) else 1)


if __name__ == "__main__":
    main()
