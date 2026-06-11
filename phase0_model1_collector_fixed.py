#!/usr/bin/env python3
"""
PHASE 0 — MODEL 1 DATASET COLLECTOR (FIXED CGROUP V2)
"""

import argparse
import csv
import json
import math
import os
import signal
import subprocess
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from threading import Semaphore, Lock
from typing import Any, Dict, List, Optional, Set, Tuple

try:
    import psutil
    HAVE_PSUTIL = True
except ImportError:
    HAVE_PSUTIL = False

# ── Output paths ───────────────────────────────────────────────────────────────
OUT_DIR    = Path.home() / "phase0-data"
OUT_DIR.mkdir(parents=True, exist_ok=True)
FINAL_CSV  = OUT_DIR / "training_dataset_model1.csv"
TRACE_DIR  = OUT_DIR / "traces"
TRACE_DIR.mkdir(exist_ok=True)
FAILED_LOG = OUT_DIR / "failed_images.log"

# ── Parallelism ────────────────────────────────────────────────────────────────
HOST_CPU_COUNT   = os.cpu_count() or 16
INITIAL_WORKERS  = min(8, HOST_CPU_COUNT // 2)
MIN_WORKERS      = 4
MAX_WORKERS      = 12
TARGET_CPU_USAGE = 75
VCPU_PER_SLOT    = 2
RUNS_PER_IMAGE   = 3

# ── CPU-drop detector ──────────────────────────────────────────────────────────
DROP_FRAC     = 0.25
HOLD_S        = 20
MIN_STARTUP_S = 10
MAX_STARTUP_S = 120
DROP_CONFIRM_S = 5
TOTAL_RUN_S    = MAX_STARTUP_S + DROP_CONFIRM_S + 5
PULL_TIMEOUT   = 180
POLL_INTERVAL  = 0.5

# ── Images (your full list here) ───────────────────────────────────────────────
ECR = "public.ecr.aws/docker/library"
MCR = "mcr.microsoft.com"

IMAGES: List[str] = [
    f"{ECR}/eclipse-temurin:21-jre-alpine",
    f"{ECR}/amazoncorretto:17-alpine",
    # Add other images as needed
]

# ══════════════════════════════════════════════════════════════════════════════
# CGROUP READERS (FIXED)
# ══════════════════════════════════════════════════════════════════════════════

class CgroupPathCache:
    def __init__(self):
        self._cpu: Dict[str, Optional[str]] = {}
        self._mem: Dict[str, Optional[str]] = {}
        self._lock = Lock()

    def _find(self, container_id: str, metric: str) -> Optional[str]:
        """Find cgroup path for a given metric using full container ID."""
        # cgroup v2 paths
        if metric == "cpu":
            path = f"/sys/fs/cgroup/system.slice/docker-{container_id}.scope/cpu.stat"
            if os.path.exists(path):
                return path
        else:  # memory
            path = f"/sys/fs/cgroup/system.slice/docker-{container_id}.scope/memory.current"
            if os.path.exists(path):
                return path
        return None

    def get_cpu_path(self, cid: str) -> Optional[str]:
        with self._lock:
            if cid not in self._cpu:
                self._cpu[cid] = self._find(cid, "cpu")
            return self._cpu[cid]

    def get_mem_path(self, cid: str) -> Optional[str]:
        with self._lock:
            if cid not in self._mem:
                self._mem[cid] = self._find(cid, "memory")
            return self._mem[cid]


cgroup_path = CgroupPathCache()


def read_cpu_usec(container_id: str) -> Optional[int]:
    """Return cumulative CPU microseconds from cgroup v2."""
    path = cgroup_path.get_cpu_path(container_id)
    if not path:
        return None
    try:
        with open(path) as f:
            for line in f:
                if line.startswith("usage_usec"):
                    return int(line.split()[1])
    except (FileNotFoundError, PermissionError, ValueError, IndexError):
        pass
    return None


def read_mem_bytes(container_id: str) -> Optional[int]:
    """Return current memory in bytes from cgroup v2."""
    path = cgroup_path.get_mem_path(container_id)
    if not path:
        return None
    try:
        with open(path) as f:
            val = int(f.read().strip())
            return val if val > 0 else None
    except (FileNotFoundError, PermissionError, ValueError):
        return None


def get_full_container_id(short_id: str) -> Optional[str]:
    """Get full 64-char container ID from short ID."""
    rc, out, _ = run(
        ["docker", "inspect", "--format", "{{.Id}}", short_id],
        timeout=5
    )
    if rc == 0 and out:
        return out.strip()
    return None


# ══════════════════════════════════════════════════════════════════════════════
# SUBPROCESS HELPER
# ══════════════════════════════════════════════════════════════════════════════

def run(cmd: List[str], timeout: int = 45, retries: int = 2) -> Tuple[int, str, str]:
    for attempt in range(retries):
        try:
            r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
            if r.returncode == 0:
                return 0, r.stdout.strip(), r.stderr.strip()
        except subprocess.TimeoutExpired:
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
        except Exception:
            pass
    return 1, "", ""


def is_java_image(image: str) -> bool:
    """Detect if image is Java-based."""
    img_lower = image.lower()
    return any(x in img_lower for x in (
        'temurin', 'openjdk', 'java', 'corretto', 'jre', 'jdk',
        'tomcat', 'gradle', 'maven', 'elasticsearch', 'logstash',
        'keycloak', 'sonarqube', 'neo4j', 'rabbitmq'
    ))


def is_python_image(image: str) -> bool:
    """Detect if image is Python-based."""
    img_lower = image.lower()
    return any(x in img_lower for x in ('python', 'jupyter', 'conda'))


def is_node_image(image: str) -> bool:
    """Detect if image is Node.js-based."""
    img_lower = image.lower()
    return any(x in img_lower for x in ('node', 'deno', 'bun', 'npm'))


# ══════════════════════════════════════════════════════════════════════════════
# TRACE COLLECTION
# ══════════════════════════════════════════════════════════════════════════════

def collect_traces(
    container_id: str,
    max_s: int = TOTAL_RUN_S,
) -> Tuple[List[float], List[float], bool]:
    """Collect CPU and memory traces via cgroup reads."""
    cpu_trace: List[float] = []
    mem_trace: List[float] = []

    prev_cpu_usec: Optional[int] = None

    # Drop detector state
    running_peak = 0.0
    candidate_start = -1
    drop_confirmed_at: Optional[int] = None

    samples_per_s = int(round(1.0 / POLL_INTERVAL))
    max_samples = max_s * samples_per_s

    for tick in range(max_samples):
        t0 = time.monotonic()
        s = tick // samples_per_s

        # Read CPU
        curr_usec = read_cpu_usec(container_id)
        if curr_usec is not None and prev_cpu_usec is not None:
            delta_usec = max(0, curr_usec - prev_cpu_usec)
            # Convert to millicores: 1 vCPU = 1,000,000 usec/s
            # Over POLL_INTERVAL seconds, max usage = POLL_INTERVAL * 1,000,000 usec
            millicores = (delta_usec / (POLL_INTERVAL * 1_000_000)) * (VCPU_PER_SLOT * 1000)
            cpu_trace.append(round(millicores, 1))
        prev_cpu_usec = curr_usec

        # Read Memory
        mem_bytes = read_mem_bytes(container_id)
        if mem_bytes is not None:
            mem_trace.append(round(mem_bytes / 1_048_576, 1))

        # CPU-drop detector (1-second aggregates)
        if cpu_trace and tick % samples_per_s == (samples_per_s - 1):
            window = cpu_trace[-samples_per_s:] if samples_per_s > 0 else [cpu_trace[-1]]
            cpu_1s = sum(window) / len(window)

            running_peak = max(running_peak, cpu_1s)

            if s >= MIN_STARTUP_S and drop_confirmed_at is None:
                threshold = DROP_FRAC * running_peak
                if cpu_1s < threshold:
                    if candidate_start == -1:
                        candidate_start = s
                    elif s - candidate_start >= HOLD_S:
                        drop_confirmed_at = candidate_start
                else:
                    candidate_start = -1

            if drop_confirmed_at is not None:
                if s - drop_confirmed_at >= DROP_CONFIRM_S:
                    return cpu_trace, mem_trace, True

        elapsed = time.monotonic() - t0
        time.sleep(max(0, POLL_INTERVAL - elapsed))

    return cpu_trace, mem_trace, False


def pct(values: List[float], p: float) -> float:
    if not values:
        return 0.0
    sv = sorted(values)
    idx = p / 100 * (len(sv) - 1)
    lo, hi = int(idx), min(int(idx) + 1, len(sv) - 1)
    return sv[lo] + (idx - lo) * (sv[hi] - sv[lo])


def summarise(cpu_raw: List[float], mem_raw: List[float]) -> Optional[Dict]:
    """Summarize traces into startup statistics."""
    if not cpu_raw or len(cpu_raw) < MIN_STARTUP_S * 2:
        return None

    # Downsample to 1-second resolution
    spx = int(round(1.0 / POLL_INTERVAL))
    cpu_1s = []
    for i in range(0, len(cpu_raw), spx):
        chunk = cpu_raw[i:i+spx]
        if chunk:
            cpu_1s.append(sum(chunk) / len(chunk))

    if not cpu_1s or max(cpu_1s) < 5.0:  # Minimum threshold for meaningful CPU
        return None

    # Detect startup end
    running_peak = 0.0
    candidate_start = -1
    startup_end = MAX_STARTUP_S

    for s, cpu in enumerate(cpu_1s):
        running_peak = max(running_peak, cpu)
        if s < MIN_STARTUP_S:
            continue
        if s >= MAX_STARTUP_S:
            startup_end = MAX_STARTUP_S
            break

        threshold = DROP_FRAC * running_peak
        if cpu < threshold:
            if candidate_start == -1:
                candidate_start = s
            elif s - candidate_start >= HOLD_S:
                startup_end = candidate_start
                break
        else:
            candidate_start = -1

    startup_cpu = cpu_1s[:startup_end]
    
    # Process memory at same resolution
    mem_1s = []
    for i in range(0, len(mem_raw), spx):
        chunk = mem_raw[i:i+spx]
        if chunk:
            mem_1s.append(max(chunk))
    startup_mem = mem_1s[:startup_end] if mem_1s else []

    result = {
        "startup_cpu_p99_m":   round(pct(startup_cpu, 99), 1),
        "startup_cpu_p95_m":   round(pct(startup_cpu, 95), 1),
        "startup_cpu_max_m":   round(max(startup_cpu), 1),
        "startup_cpu_mean_m":  round(sum(startup_cpu) / len(startup_cpu), 1),
        "startup_duration_s":  float(startup_end),
        "startup_mem_peak_mb": round(max(startup_mem), 1) if startup_mem else "",
        "startup_mem_mean_mb": round(sum(startup_mem) / len(startup_mem), 1) if startup_mem else "",
        "startup_mem_at_end_mb": round(startup_mem[-1], 1) if startup_mem else "",
    }
    return result


# ══════════════════════════════════════════════════════════════════════════════
# CONTAINER BENCHMARK
# ══════════════════════════════════════════════════════════════════════════════

def benchmark_image(image: str, run_idx: int) -> Optional[Dict]:
    """Run one container and collect traces."""
    # Pull image
    rc, _, err = run(["docker", "pull", image], timeout=PULL_TIMEOUT, retries=2)
    if rc != 0:
        print(f"    Pull failed: {err[:60]}")
        return None

    container_name = f"p0-{abs(hash(image + str(run_idx))) % 100000:05d}"
    subprocess.run(["docker", "rm", "-f", container_name], capture_output=True, timeout=8)

    # Determine if we should keep container alive or let it run normally
    needs_keepalive = is_python_image(image) or is_node_image(image)
    
    # For Java images, let the default entrypoint run (no override)
    # They will start JVM and consume CPU naturally
    if is_java_image(image):
        # Java images: run normally, they exit after their task
        # Use a long-running Java process if available, or let default run
        rc, container_id, _ = run(
            ["docker", "run", "-d",
             f"--cpus={VCPU_PER_SLOT}", "--memory=4g",
             "--name", container_name, image],
            timeout=20
        )
        if rc != 0:
            return None
    elif needs_keepalive:
        # Python/Node: need to keep alive with a web server
        if is_python_image(image):
            cmd = ["python3", "-m", "http.server", "8000"]
        else:  # Node
            cmd = ["node", "-e", "require('http').createServer((req,res)=>res.end('ok')).listen(3000)"]
        
        rc, container_id, _ = run(
            ["docker", "run", "-d",
             f"--cpus={VCPU_PER_SLOT}", "--memory=4g",
             "--name", container_name, image] + cmd,
            timeout=20
        )
        if rc != 0:
            return None
    else:
        # Default: keep alive with tail
        rc, container_id, _ = run(
            ["docker", "run", "-d",
             f"--cpus={VCPU_PER_SLOT}", "--memory=4g",
             "--name", container_name, image,
             "tail", "-f", "/dev/null"],
            timeout=20
        )
        if rc != 0:
            return None

    # Get full container ID
    full_id = get_full_container_id(container_id.strip())
    if not full_id:
        subprocess.run(["docker", "rm", "-f", container_name], capture_output=True, timeout=8)
        return None

    # Wait for cgroup to be ready
    time.sleep(2)

    # Collect traces
    try:
        cpu_trace, mem_trace, early_stop = collect_traces(full_id)
        return summarise(cpu_trace, mem_trace)
    finally:
        subprocess.run(["docker", "rm", "-f", container_name], capture_output=True, timeout=8)


# ══════════════════════════════════════════════════════════════════════════════
# METADATA EXTRACTION (simplified)
# ══════════════════════════════════════════════════════════════════════════════

def extract_static_features(image: str) -> Dict:
    """Extract basic static features."""
    parts = image.split(":", 1)
    return {
        "image": image,
        "image_tag": parts[1] if len(parts) > 1 else "latest",
        "image_repo": parts[0].split("/")[0] if "/" in parts[0] else "library",
        "lang_java": 1 if is_java_image(image) else 0,
        "lang_python": 1 if is_python_image(image) else 0,
        "lang_js": 1 if is_node_image(image) else 0,
    }


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--fast", action="store_true")
    parser.add_argument("--discard-first", action="store_true")
    parser.add_argument("--workers", type=int, default=INITIAL_WORKERS)
    parser.add_argument("--runs", type=int, default=RUNS_PER_IMAGE)
    parser.add_argument("--output", type=Path, default=FINAL_CSV)
    parser.add_argument("--images", nargs="+", help="Specific images")
    args = parser.parse_args()

    if args.fast:
        args.runs = 1

    # Image list
    if args.images:
        todo = args.images
    else:
        todo = IMAGES[:2]  # Test with 2 images

    print(f"\n{'═'*70}")
    print(f"PHASE 0 MODEL 1 COLLECTOR — CGROUP V2 (FIXED)")
    print(f"{'═'*70}")
    print(f"  Images: {len(todo)}")
    print(f"  Runs per image: {args.runs}")
    print(f"  Output: {args.output}")
    print(f"{'═'*70}\n")

    # Collect metadata
    static_map = {}
    for img in todo:
        static_map[img] = extract_static_features(img)

    # Collect runtime data
    results = []
    for idx, image in enumerate(todo, 1):
        print(f"\n[{idx}/{len(todo)}] {image}")
        
        summaries = []
        for run_idx in range(1, args.runs + 1):
            if args.discard_first and run_idx == 1:
                print(f"  Run {run_idx}: warming up...", end=" ", flush=True)
            else:
                print(f"  Run {run_idx}: collecting...", end=" ", flush=True)
            
            t0 = time.time()
            result = benchmark_image(image, run_idx)
            elapsed = time.time() - t0
            
            if result:
                summaries.append(result)
                print(f"OK ({elapsed:.0f}s) - p99={result['startup_cpu_p99_m']}m, mem={result['startup_mem_peak_mb']}MB")
            else:
                print(f"FAILED ({elapsed:.0f}s)")

        if summaries:
            # Average results
            row = dict(static_map[image])
            row["runs_ok"] = len(summaries)
            for key in summaries[0].keys():
                values = [s[key] for s in summaries if s.get(key) not in ("", None)]
                if values and isinstance(values[0], (int, float)):
                    row[key] = round(sum(values) / len(values), 1)
                elif values:
                    row[key] = values[0]
            results.append(row)

    # Write CSV
    if results:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        with open(args.output, "w", newline="") as f:
            fieldnames = ["image", "image_tag", "image_repo", "runs_ok",
                         "startup_cpu_p99_m", "startup_cpu_p95_m", "startup_cpu_max_m",
                         "startup_cpu_mean_m", "startup_duration_s",
                         "startup_mem_peak_mb", "startup_mem_mean_mb", "startup_mem_at_end_mb",
                         "lang_java", "lang_python", "lang_js"]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(results)

        print(f"\n{'═'*70}")
        print(f"COLLECTION COMPLETE")
        print(f"  {len(results)}/{len(todo)} images labelled")
        print(f"  Output: {args.output}")
        print(f"{'═'*70}\n")

        # Show results
        for r in results:
            print(f"{r['image'].split('/')[-1][:35]:35}  "
                  f"p99={r.get('startup_cpu_p99_m','?')}m  "
                  f"mem={r.get('startup_mem_peak_mb','?')}MB  "
                  f"runs={r['runs_ok']}")
    else:
        print("\n  No data collected!")


if __name__ == "__main__":
    signal.signal(signal.SIGINT, lambda s, f: sys.exit(0))
    main()
