#!/usr/bin/env python3
"""
PHASE 0 — MODEL 1 DATASET COLLECTOR (PRODUCTION - ALL 114 IMAGES)
"""

import argparse
import csv
import json
import os
import signal
import subprocess
import sys
import time
import threading
from datetime import datetime, timezone
from pathlib import Path
from threading import Semaphore, Lock
from typing import Dict, List, Optional, Tuple

# ── Configuration ──────────────────────────────────────────────────────────────
OUT_DIR = Path.home() / "phase0-data"
OUT_DIR.mkdir(parents=True, exist_ok=True)
TRACE_DIR = OUT_DIR / "traces"
TRACE_DIR.mkdir(exist_ok=True)

VCPU_PER_SLOT = 2
RUNS_PER_IMAGE = 3
DROP_FRAC = 0.25
HOLD_S = 20
MIN_STARTUP_S = 10
MAX_STARTUP_S = 120
DROP_CONFIRM_S = 5
TOTAL_RUN_S = MAX_STARTUP_S + DROP_CONFIRM_S + 5
PULL_TIMEOUT = 180
POLL_INTERVAL = 0.1

# Parallelism
HOST_CPU_COUNT = os.cpu_count() or 16
INITIAL_WORKERS = min(8, HOST_CPU_COUNT // 2)

ECR = "public.ecr.aws/docker/library"
MCR = "mcr.microsoft.com"

# Full 114 images
IMAGES: List[str] = [
    "public.ecr.aws/elastic/elasticsearch:8.11.0",
    "public.ecr.aws/elastic/elasticsearch:8.9.0",
    "public.ecr.aws/elastic/elasticsearch:7.17.16",
    "public.ecr.aws/elastic/logstash:8.11.0",
    f"{ECR}/tomcat:10-jre21-temurin-jammy",
    f"{ECR}/tomcat:10-jre17-temurin-jammy",
    f"{ECR}/tomcat:9-jre17-temurin-jammy",
    f"{ECR}/gradle:8-jdk21-alpine",
    f"{ECR}/gradle:8-jdk17-alpine",
    f"{ECR}/maven:3.9-eclipse-temurin-21-alpine",
    f"{ECR}/maven:3.9-eclipse-temurin-17-alpine",
    f"{ECR}/maven:3.8-eclipse-temurin-11-alpine",
    f"{ECR}/rabbitmq:3-management-alpine",
    f"{ECR}/neo4j:5-community",
    f"{ECR}/sonarqube:10-community",
    f"{ECR}/eclipse-temurin:21-jre-alpine",
    f"{ECR}/eclipse-temurin:21-alpine",
    f"{ECR}/eclipse-temurin:21-jdk-alpine",
    f"{ECR}/eclipse-temurin:17-jre-alpine",
    f"{ECR}/eclipse-temurin:17-alpine",
    f"{ECR}/eclipse-temurin:11-jre-alpine",
    f"{ECR}/eclipse-temurin:11-alpine",
    f"{ECR}/eclipse-temurin:8-jre-alpine",
    f"{ECR}/eclipse-temurin:8-alpine",
    "public.ecr.aws/amazoncorretto/amazoncorretto:21",
    f"{ECR}/amazoncorretto:17-alpine",
    f"{ECR}/amazoncorretto:11-alpine",
    f"{ECR}/amazoncorretto:8-alpine",
    f"{ECR}/eclipse-temurin:21-jammy",
    f"{ECR}/eclipse-temurin:17-jammy",
    f"{ECR}/eclipse-temurin:11-jammy",
    f"{ECR}/groovy:4",
    f"{ECR}/groovy:3",
    f"{ECR}/clojure:temurin-21-alpine",
    f"{ECR}/clojure:temurin-17-alpine",
    f"{ECR}/gradle:8-jdk11-alpine",
    f"{ECR}/amazoncorretto:21-alpine",
    f"{MCR}/dotnet/aspnet:8.0-alpine3.18",
    f"{MCR}/dotnet/aspnet:7.0-alpine3.18",
    f"{MCR}/dotnet/aspnet:6.0-alpine3.18",
    f"{MCR}/dotnet/runtime:8.0-alpine3.18",
    f"{MCR}/dotnet/runtime:7.0-alpine3.18",
    f"{MCR}/dotnet/runtime:6.0-alpine3.18",
    f"{MCR}/dotnet/sdk:8.0-alpine3.18",
    f"{MCR}/dotnet/sdk:7.0-alpine3.18",
    f"{ECR}/mono:6-slim",
    f"{ECR}/mono:6",
    f"{ECR}/python:3.12",
    f"{ECR}/python:3.12-slim",
    f"{ECR}/python:3.12-alpine",
    f"{ECR}/python:3.11",
    f"{ECR}/python:3.11-slim",
    f"{ECR}/python:3.11-alpine",
    f"{ECR}/python:3.10-slim",
    f"{ECR}/python:3.9-slim",
    f"{ECR}/node:20-alpine",
    f"{ECR}/node:20-slim",
    f"{ECR}/node:20",
    f"{ECR}/node:18-alpine",
    f"{ECR}/node:18-slim",
    f"{ECR}/node:16-alpine",
    f"{ECR}/ruby:3.3-alpine",
    f"{ECR}/ruby:3.2-alpine",
    f"{ECR}/ruby:3.1-alpine",
    f"{ECR}/perl:5-slim",
    f"{ECR}/swift:5.9-slim",
    f"{ECR}/julia:1.10-alpine",
    f"{ECR}/r-base:4.0.3",
    f"{ECR}/golang:1.22-alpine",
    f"{ECR}/golang:1.22-bookworm",
    f"{ECR}/golang:1.21-alpine",
    f"{ECR}/golang:1.20-alpine",
    f"{ECR}/traefik:v3.0",
    f"{ECR}/traefik:v2.11",
    f"{ECR}/caddy:2-alpine",
    f"{ECR}/consul:1.6",
    "public.ecr.aws/hashicorp/consul:1.17",
    "public.ecr.aws/hashicorp/vault:1.15",
    f"{ECR}/rust:1.76-alpine",
    f"{ECR}/rust:1.75-slim",
    f"{ECR}/rust:1.76",
    f"{ECR}/gcc:13-bookworm",
    f"{ECR}/gcc:12-bookworm",
    f"{ECR}/gcc:11-bookworm",
    f"{ECR}/swift:5.9",
    f"{ECR}/haproxy:2.9-alpine",
    f"{ECR}/nginx:1.25-alpine",
    f"{ECR}/nginx:1.24-alpine",
    f"{ECR}/nginx:alpine",
    f"{ECR}/nginx:1.25-bookworm",
    f"{ECR}/httpd:2.4-alpine",
    f"{ECR}/httpd:alpine",
    f"{ECR}/redis:7-alpine",
    f"{ECR}/redis:7.2-alpine",
    f"{ECR}/redis:6-alpine",
    f"{ECR}/memcached:alpine",
    f"{ECR}/memcached:1.6-alpine",
    f"{ECR}/varnish:7-alpine",
    f"{ECR}/haproxy:2.8-alpine",
    f"{ECR}/nginx:stable-alpine",
    f"{ECR}/busybox:1.36",
    f"{ECR}/alpine:3.19",
    f"{ECR}/alpine:3.18",
    f"{ECR}/alpine:3.17",
    f"{ECR}/debian:bookworm-slim",
    f"{ECR}/ubuntu:22.04",
    f"{ECR}/postgres:16-alpine",
    f"{ECR}/postgres:15-alpine",
    f"{ECR}/postgres:14-alpine",
    f"{ECR}/mysql:8.0",
    f"{ECR}/mysql:8.2",
    f"{ECR}/mariadb:11-jammy",
    f"{ECR}/mongo:7",
    f"{ECR}/mongo:6",
]

CSV_COLUMNS = [
    "image", "collected_at", "runs_ok",
    "startup_cpu_p99_m", "startup_cpu_p95_m", "startup_cpu_max_m",
    "startup_cpu_mean_m", "startup_duration_s",
    "startup_mem_peak_mb", "startup_mem_mean_mb", "startup_mem_at_end_mb",
]


class CgroupPathCache:
    def __init__(self):
        self._paths = {}
        self._lock = Lock()

    def get_cpu_path(self, container_id: str) -> Optional[str]:
        with self._lock:
            if container_id not in self._paths:
                path = f"/sys/fs/cgroup/system.slice/docker-{container_id}.scope/cpu.stat"
                if os.path.exists(path):
                    self._paths[container_id] = path
                else:
                    self._paths[container_id] = None
            return self._paths[container_id]

    def get_mem_path(self, container_id: str) -> Optional[str]:
        with self._lock:
            key = f"{container_id}_mem"
            if key not in self._paths:
                path = f"/sys/fs/cgroup/system.slice/docker-{container_id}.scope/memory.current"
                if os.path.exists(path):
                    self._paths[key] = path
                else:
                    self._paths[key] = None
            return self._paths[key]


cgroup_cache = CgroupPathCache()


def read_cpu_usec(container_id: str) -> Optional[int]:
    path = cgroup_cache.get_cpu_path(container_id)
    if not path:
        return None
    try:
        with open(path) as f:
            for line in f:
                if line.startswith("usage_usec"):
                    return int(line.split()[1])
    except Exception:
        pass
    return None


def read_mem_bytes(container_id: str) -> Optional[int]:
    path = cgroup_cache.get_mem_path(container_id)
    if not path:
        return None
    try:
        with open(path) as f:
            return int(f.read().strip())
    except Exception:
        return None


def run_cmd(cmd: List[str], timeout: int = 30) -> Tuple[int, str, str]:
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return r.returncode, r.stdout.strip(), r.stderr.strip()
    except Exception:
        return 1, "", ""


def collect_traces(container_id: str, debug: bool = False) -> Tuple[List[float], List[float], bool]:
    cpu_trace = []
    mem_trace = []
    
    prev_cpu = read_cpu_usec(container_id)
    
    max_delta_per_vcpu = POLL_INTERVAL * 1_000_000
    max_delta_total = max_delta_per_vcpu * VCPU_PER_SLOT
    
    running_peak = 0.0
    candidate_start = -1
    drop_confirmed_at = None
    
    samples_per_s = int(1.0 / POLL_INTERVAL)
    max_samples = TOTAL_RUN_S * samples_per_s
    
    for tick in range(max_samples):
        t0 = time.time()
        s = tick // samples_per_s
        
        curr_cpu = read_cpu_usec(container_id)
        if curr_cpu is not None and prev_cpu is not None and curr_cpu > prev_cpu:
            delta = curr_cpu - prev_cpu
            millicores = (delta / max_delta_total) * VCPU_PER_SLOT * 1000
            cpu_trace.append(round(millicores, 1))
        
        prev_cpu = curr_cpu
        
        mem = read_mem_bytes(container_id)
        if mem:
            mem_trace.append(round(mem / 1_048_576, 1))
        
        if len(cpu_trace) >= samples_per_s and tick % samples_per_s == (samples_per_s - 1):
            window = cpu_trace[-samples_per_s:]
            cpu_1s = sum(window) / len(window) if window else 0
            running_peak = max(running_peak, cpu_1s)
            
            if s >= MIN_STARTUP_S and drop_confirmed_at is None and running_peak > 0:
                threshold = DROP_FRAC * running_peak
                if cpu_1s < threshold:
                    if candidate_start == -1:
                        candidate_start = s
                    elif s - candidate_start >= HOLD_S:
                        drop_confirmed_at = candidate_start
                else:
                    candidate_start = -1
            
            if drop_confirmed_at and s - drop_confirmed_at >= DROP_CONFIRM_S:
                return cpu_trace, mem_trace, True
        
        elapsed = time.time() - t0
        time.sleep(max(0, POLL_INTERVAL - elapsed))
    
    return cpu_trace, mem_trace, False


def summarise(cpu_raw: List[float], mem_raw: List[float]) -> Dict:
    if not cpu_raw:
        return {
            "startup_cpu_p99_m": 0.0,
            "startup_cpu_p95_m": 0.0,
            "startup_cpu_max_m": 0.0,
            "startup_cpu_mean_m": 0.0,
            "startup_duration_s": float(MIN_STARTUP_S),
            "startup_mem_peak_mb": 0.0,
            "startup_mem_mean_mb": 0.0,
            "startup_mem_at_end_mb": 0.0,
        }
    
    max_cpu = max(cpu_raw)
    
    # For very low CPU, return simple stats
    if max_cpu < 1.0:
        return {
            "startup_cpu_p99_m": round(max_cpu, 1),
            "startup_cpu_p95_m": round(max_cpu, 1),
            "startup_cpu_max_m": round(max_cpu, 1),
            "startup_cpu_mean_m": round(sum(cpu_raw) / len(cpu_raw), 1),
            "startup_duration_s": float(MIN_STARTUP_S),
            "startup_mem_peak_mb": round(max(mem_raw), 1) if mem_raw else 0.0,
            "startup_mem_mean_mb": round(sum(mem_raw) / len(mem_raw), 1) if mem_raw else 0.0,
            "startup_mem_at_end_mb": round(mem_raw[-1], 1) if mem_raw else 0.0,
        }
    
    spx = int(1.0 / POLL_INTERVAL)
    cpu_1s = []
    for i in range(0, len(cpu_raw), spx):
        chunk = cpu_raw[i:i+spx]
        if chunk:
            cpu_1s.append(sum(chunk) / len(chunk))
    
    if not cpu_1s:
        return {
            "startup_cpu_p99_m": round(max_cpu, 1),
            "startup_cpu_p95_m": round(max_cpu, 1),
            "startup_cpu_max_m": round(max_cpu, 1),
            "startup_cpu_mean_m": round(sum(cpu_raw) / len(cpu_raw), 1),
            "startup_duration_s": float(MIN_STARTUP_S),
            "startup_mem_peak_mb": round(max(mem_raw), 1) if mem_raw else 0.0,
            "startup_mem_mean_mb": round(sum(mem_raw) / len(mem_raw), 1) if mem_raw else 0.0,
            "startup_mem_at_end_mb": round(mem_raw[-1], 1) if mem_raw else 0.0,
        }
    
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
        
        if running_peak > 0:
            threshold = DROP_FRAC * running_peak
            if cpu < threshold:
                if candidate_start == -1:
                    candidate_start = s
                elif s - candidate_start >= HOLD_S:
                    startup_end = candidate_start
                    break
            else:
                candidate_start = -1
    
    startup_end = max(MIN_STARTUP_S, startup_end)
    startup_cpu = cpu_raw[:startup_end * spx]
    startup_mem = mem_raw[:startup_end * spx] if mem_raw else []
    
    def pct(vals, p):
        if not vals:
            return 0.0
        sv = sorted(vals)
        idx = p / 100 * (len(sv) - 1)
        lo, hi = int(idx), min(int(idx) + 1, len(sv) - 1)
        return sv[lo] + (idx - lo) * (sv[hi] - sv[lo])
    
    return {
        "startup_cpu_p99_m": round(pct(startup_cpu, 99), 1),
        "startup_cpu_p95_m": round(pct(startup_cpu, 95), 1),
        "startup_cpu_max_m": round(max(startup_cpu), 1),
        "startup_cpu_mean_m": round(sum(startup_cpu) / len(startup_cpu), 1),
        "startup_duration_s": float(startup_end),
        "startup_mem_peak_mb": round(max(startup_mem), 1) if startup_mem else 0.0,
        "startup_mem_mean_mb": round(sum(startup_mem) / len(startup_mem), 1) if startup_mem else 0.0,
        "startup_mem_at_end_mb": round(startup_mem[-1], 1) if startup_mem else 0.0,
    }


def get_container_strategy(image: str) -> Tuple[List[str], bool]:
    """Get the appropriate docker run command for an image."""
    img_lower = image.lower()
    
    # Real applications that have daemons
    if any(x in img_lower for x in ['elasticsearch', 'tomcat', 'gradle', 'maven', 
                                      'rabbitmq', 'neo4j', 'sonarqube', 'dotnet', 
                                      'postgres', 'mysql', 'redis', 'nginx', 'httpd',
                                      'traefik', 'caddy', 'consul', 'vault', 'mongo',
                                      'mariadb', 'memcached', 'varnish', 'haproxy']):
        return ([
            "docker", "run", "-d",
            f"--cpus={VCPU_PER_SLOT}", "--memory=4g",
            "--name", "{name}", image
        ], False)
    
    # Python - run HTTP server
    elif 'python' in img_lower:
        return ([
            "docker", "run", "-d",
            f"--cpus={VCPU_PER_SLOT}", "--memory=4g",
            "--name", "{name}", image,
            "python3", "-m", "http.server", "8000"
        ], True)
    
    # Node.js - run HTTP server
    elif 'node' in img_lower:
        return ([
            "docker", "run", "-d",
            f"--cpus={VCPU_PER_SLOT}", "--memory=4g",
            "--name", "{name}", image,
            "node", "-e", 
            "require('http').createServer((req,res)=>res.end('ok')).listen(3000)"
        ], True)
    
    # Ruby - run web server
    elif 'ruby' in img_lower:
        return ([
            "docker", "run", "-d",
            f"--cpus={VCPU_PER_SLOT}", "--memory=4g",
            "--name", "{name}", image,
            "ruby", "-run", "-e", "httpd", ".", "-p", "8000"
        ], True)
    
    # PHP - run web server
    elif 'php' in img_lower:
        return ([
            "docker", "run", "-d",
            f"--cpus={VCPU_PER_SLOT}", "--memory=4g",
            "--name", "{name}", image,
            "php", "-S", "0.0.0.0:8000"
        ], True)
    
    # Default: run a minimal CPU loop to keep container active
    else:
        keepalive = "while true; do i=0; while [ $i -lt 10000 ]; do i=$((i+1)); done; sleep 0.1; done"
        return ([
            "docker", "run", "-d",
            f"--cpus={VCPU_PER_SLOT}", "--memory=4g",
            "--entrypoint", "/bin/sh",
            "--name", "{name}", image,
            "-c", keepalive
        ], True)


def benchmark_image(image: str, run_idx: int, debug: bool = False) -> Optional[Dict]:
    container_name = f"p0-{abs(hash(image + str(run_idx))) % 100000:05d}"
    subprocess.run(["docker", "rm", "-f", container_name], capture_output=True)
    
    # Pull image
    run_cmd(["docker", "pull", image], timeout=PULL_TIMEOUT)
    
    # Get strategy
    cmd_template, needs_shell = get_container_strategy(image)
    
    # Replace name placeholder
    cmd = [x.format(name=container_name) if '{name}' in x else x for x in cmd_template]
    
    # Try with /bin/sh first, then fallback to sh for Alpine
    if needs_shell:
        # Try with /bin/sh
        rc, container_id, _ = run_cmd(cmd, timeout=30)
        
        # If failed, try with sh (Alpine)
        if rc != 0 or not container_id:
            cmd[cmd.index("/bin/sh")] = "sh"
            rc, container_id, _ = run_cmd(cmd, timeout=30)
    else:
        rc, container_id, _ = run_cmd(cmd, timeout=30)
    
    if rc != 0 or not container_id:
        return None
    
    # Get full ID
    _, full_id, _ = run_cmd(["docker", "inspect", "--format", "{{.Id}}", container_id])
    if not full_id:
        subprocess.run(["docker", "rm", "-f", container_name], capture_output=True)
        return None
    
    time.sleep(0.5)
    
    try:
        cpu_trace, mem_trace, early = collect_traces(full_id, debug=debug)
        return summarise(cpu_trace, mem_trace)
    finally:
        subprocess.run(["docker", "rm", "-f", container_name], capture_output=True)


def process_image(image: str, runs: int, discard_first: bool, debug: bool, 
                  results: List, lock: Lock, idx: int, total: int):
    print(f"[{idx}/{total}] Processing {image.split('/')[-1][:50]}...")
    
    summaries = []
    for run_idx in range(1, runs + 1):
        if discard_first and run_idx == 1:
            print(f"  Run {run_idx}: warming up...", end=" ", flush=True)
        else:
            print(f"  Run {run_idx}: collecting...", end=" ", flush=True)
        
        t0 = time.time()
        result = benchmark_image(image, run_idx, debug=debug)
        elapsed = time.time() - t0
        
        if discard_first and run_idx == 1:
            print(f"done ({elapsed:.0f}s)")
        elif result:
            summaries.append(result)
            print(f"OK ({elapsed:.0f}s) - p99={result['startup_cpu_p99_m']}m, mem={result['startup_mem_peak_mb']}MB")
        else:
            print(f"FAILED ({elapsed:.0f}s)")
    
    if summaries:
        row = {
            "image": image,
            "collected_at": datetime.now(timezone.utc).isoformat(),
            "runs_ok": len(summaries),
        }
        for key in summaries[0].keys():
            values = [s[key] for s in summaries]
            if isinstance(values[0], (int, float)):
                row[key] = round(sum(values) / len(values), 1)
            else:
                row[key] = values[0] if values else ""
        
        with lock:
            results.append(row)
        print(f"  ✓ Completed: p99={row['startup_cpu_p99_m']}m, duration={row['startup_duration_s']}s")
    else:
        print(f"  ✗ No valid data for {image}")


def main():
    parser = argparse.ArgumentParser(description="Phase 0 Model 1 Collector")
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--output", type=Path, default=OUT_DIR / "training_dataset_model1.csv")
    parser.add_argument("--images", nargs="+", help="Specific images to test")
    parser.add_argument("--workers", type=int, default=INITIAL_WORKERS)
    parser.add_argument("--runs", type=int, default=RUNS_PER_IMAGE)
    parser.add_argument("--discard-first", action="store_true")
    parser.add_argument("--fast", action="store_true")
    parser.add_argument("--resume", action="store_true")
    args = parser.parse_args()
    
    if args.fast:
        args.runs = 1
        args.discard_first = False
    
    if args.images:
        todo = args.images
    else:
        todo = IMAGES
    
    if args.resume and args.output.exists():
        existing = set()
        with open(args.output) as f:
            for r in csv.DictReader(f):
                existing.add(r.get("image", ""))
        todo = [img for img in todo if img not in existing]
        print(f"Resuming: {len(todo)} images remaining")
    
    print(f"\n{'═'*70}")
    print(f"PHASE 0 MODEL 1 COLLECTOR (PRODUCTION - ALL 114 IMAGES)")
    print(f"{'═'*70}")
    print(f"  Images: {len(todo)}")
    print(f"  Workers: {args.workers}")
    print(f"  Runs per image: {args.runs}")
    print(f"  Discard first: {args.discard_first}")
    print(f"  Output: {args.output}")
    print(f"{'═'*70}\n")
    
    results = []
    lock = Lock()
    threads = []
    sem = Semaphore(args.workers)
    
    def process_with_semaphore(image, idx, total):
        with sem:
            process_image(image, args.runs, args.discard_first, args.debug, 
                         results, lock, idx, total)
    
    for idx, image in enumerate(todo, 1):
        t = threading.Thread(target=process_with_semaphore, args=(image, idx, len(todo)))
        threads.append(t)
        t.start()
        time.sleep(0.3)
    
    for t in threads:
        t.join()
    
    if results:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        with open(args.output, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(results)
        
        print(f"\n{'═'*70}")
        print(f"COLLECTION COMPLETE")
        print(f"  {len(results)}/{len(todo)} images labelled")
        print(f"  Output: {args.output}")
        print(f"{'═'*70}\n")
        
        print("Results preview:")
        for r in results[:10]:
            print(f"  {r['image'].split('/')[-1][:45]:45} "
                  f"p99={r.get('startup_cpu_p99_m', 0):>6.1f}m  "
                  f"mem={r.get('startup_mem_peak_mb', 0):>6.1f}MB  "
                  f"dur={r.get('startup_duration_s', 0):>4.0f}s")
    else:
        print("\n  No data collected!")


if __name__ == "__main__":
    signal.signal(signal.SIGINT, lambda s, f: sys.exit(0))
    main()
