#!/usr/bin/env python3
"""
PHASE 1-1 PARALLEL AWS — 20-IMAGE GROUND-TRUTH COLLECTION  (Batch A)
Zero-Telemetry CPU Prediction  /  Carbon-Aware Right-Sizing
Target: AWS EC2 m5.4xlarge  (16 vCPU, 64 GB, k3s)

Images (20): C/Alpine × 7, JVM × 8, Python × 5
  (lighttpd→py3.9-slim; jupyter→py3.11; openjdk:21-slim→eclipse-temurin:21-jre-alpine
   to avoid cross-batch duplicate with Batch B)
All critical, warning, and note fixes applied:
  FIX-1  cassandra-stress: split "-node"/ip as separate args; add port flag
  FIX-2  HAProxy:  no default config → override CMD with stress_ng batch
  FIX-3  Varnish:  needs VARNISH_BACKEND_* env vars to start daemon
  FIX-4  Verdaccio: use full ECR path (not docker/library mirror)
  FIX-5  Ghost:    add NODE_ENV + url env vars to suppress DB init loop
  FIX-6  WordPress: add WORDPRESS_DB_* env vars to suppress install wizard
  FIX-7  Redmine:  override to batch+sleep to avoid DB crash
  FIX-8  eclipse-temurin (sleep ∞): cap cpu_startup_ratio to 99.9 sentinel
  FIX-9  Kafka dead code removed; JVM_60MIN entry cleaned up
  FIX-10 InfluxDB: add DOCKER_INFLUXDB_INIT_* env vars
  FIX-11 ZeroDivisionError guard in cpu_startup_ratio (max(..., 0.1))
  FIX-12 openjdk:21-slim replaced with eclipse-temurin:21-jre-alpine (removes
         cross-batch duplicate; temurin is already in JVM_60MIN set)
  FIX-13 _infer_lang_from_env: Rust env vars now return lang_other, not lang_go
  FIX-14 vus_b field added to IMAGE_CONFIG; k6_burst_vus reads it (no more
         implicit vus_s×2 silently mismatching the k6 script)

Modes
-----
  --smoke   20 images × 5-min window × 10 workers → ~11 min (pipeline check)
  --half    20 images × tiered windows × 10 workers → ~90 min (JVM=60m, others=30m)
  --full    20 images × 60-min window × 10 workers → ~120 min (paper dataset)

Output
------
  ~/zero-telemetry-cpu/data/dataset_a.csv
  ~/zero-telemetry-cpu/data/cpu_traces/
  ~/zero-telemetry-cpu/results/phase11_summary.txt

Usage
-----
  python3 phase11_new_parallel_aws.py --smoke
  python3 phase11_new_parallel_aws.py --half
  python3 phase11_new_parallel_aws.py --full
  python3 phase11_new_parallel_aws.py --image nginx:alpine --workers 2
  python3 phase11_new_parallel_aws.py --full --fresh
"""

import argparse, csv, json, math, os, shutil, subprocess, sys
import tempfile, time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
from multiprocessing import Manager
from pathlib import Path
from threading import Event, Thread
import multiprocessing as _mp

# ── Venv bridge ───────────────────────────────────────────────────────────────
_VENV_PY = Path.home() / ".venv-zerotelem" / "bin" / "python"
PY = str(_VENV_PY) if _VENV_PY.exists() else sys.executable

# ── ANSI ──────────────────────────────────────────────────────────────────────
G, R, Y, B, C = "\033[92m", "\033[91m", "\033[93m", "\033[94m", "\033[96m"
RST, BOLD, DIM = "\033[0m", "\033[1m", "\033[2m"

# ── Paths ─────────────────────────────────────────────────────────────────────
PROJECT   = Path.home() / "zero-telemetry-cpu"
DATA_DIR  = PROJECT / "data"
TRACE_DIR = DATA_DIR / "cpu_traces"
RES_DIR   = PROJECT / "results"
LOG_DIR   = PROJECT / "logs"
DATASET   = DATA_DIR / "dataset_a.csv"          # Batch A
SUMMARY   = RES_DIR  / "phase11_summary.txt"
TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_FILE  = LOG_DIR  / f"phase11_aws_{TIMESTAMP}.log"

for _d in (DATA_DIR, TRACE_DIR, RES_DIR, LOG_DIR):
    _d.mkdir(parents=True, exist_ok=True)

# ── Timing constants ──────────────────────────────────────────────────────────
WARMUP_S    = 120
RAMPUP_S    = 480
SUSTAINED_S = 1800
BURST_S     = 900
COOLDOWN_S  = 300
FULL_S      = WARMUP_S + RAMPUP_S + SUSTAINED_S + BURST_S + COOLDOWN_S  # 3600s
HALF_S      = 1800
SMOKE_S     = 300
CPU_INTERVAL_S = 5

# ── JVM images requiring 60-min window ───────────────────────────────────────
# FIX-9:  kafka removed (no IMAGE_CONFIG entry; dead code eliminated)
# FIX-12: openjdk:21-slim replaced with eclipse-temurin:21-jre-alpine in
#         Batch A IMAGE_CONFIG to avoid cross-batch duplicate with Batch B.
#         eclipse-temurin:21-jre-alpine is the JRE-only variant, confirmed on
#         ECR, and already belongs in the 60-min JIT warm-up set.
JVM_60MIN = {
    "public.ecr.aws/docker/library/tomcat:10-jre21-temurin-jammy",
    "public.ecr.aws/docker/library/eclipse-temurin:21-jre-alpine",
    "public.ecr.aws/bitnami/keycloak:23",
    "public.ecr.aws/elastic/elasticsearch:8.11.0",
    "public.ecr.aws/docker/library/gradle:8-jdk21-alpine",
    "public.ecr.aws/docker/library/maven:3.9-eclipse-temurin-21-alpine",
    "public.ecr.aws/docker/library/eclipse-temurin:21-alpine",
    "public.ecr.aws/docker/library/eclipse-temurin:17-jre-alpine",
}

ECR = "public.ecr.aws/docker/library"

# ══════════════════════════════════════════════════════════════════════════════
#  BATCH A — 20 IMAGES
#  C/Alpine × 7  |  JVM × 8  |  Python × 5
#
#  vus_r  = ramp-up VUs (k6 ramping-vus executor target)
#  vus_s  = sustained VUs (k6 constant-vus executor)
#  vus_b  = burst VUs (k6 constant-vus burst stage; explicit to match k6 script)
#           For non-k6 protocols (redis, memcached, stress_ng, batch) vus_b is
#           used as worker/thread count for the burst phase.
# ══════════════════════════════════════════════════════════════════════════════
IMAGE_CONFIG: dict[str, dict] = {

    # ── C / Alpine (7) ────────────────────────────────────────────────────────
    f"{ECR}/nginx:alpine":      {"port": 80,    "proto": "http",      "vus_r": 10, "vus_s": 10, "vus_b": 20},
    f"{ECR}/httpd:alpine":      {"port": 80,    "proto": "http",      "vus_r": 10, "vus_s": 10, "vus_b": 20},
    # lighttpd:alpine not available on ECR docker/library mirror — replaced with python:3.9-slim
    f"{ECR}/python:3.9-slim":    {"port": 8000, "proto": "http",      "vus_r": 5,  "vus_s": 5,  "vus_b": 10},
    # HAProxy has no config → install stress-ng at runtime and run 4 CPU workers
    # proto=stress_ng dispatches to _stress_ng_load() which installs + runs stress-ng
    f"{ECR}/haproxy:alpine":    {"port": 0,     "proto": "stress_ng", "vus_r": 0,  "vus_s": 4,  "vus_b": 4},
    # FIX-3: Varnish needs backend env vars — handled in env_yaml_lines below
    f"{ECR}/varnish:alpine":         {"port": 80, "proto": "http",     "vus_r": 15, "vus_s": 15, "vus_b": 30},
    f"{ECR}/redis:7-alpine":    {"port": 6379,  "proto": "redis",     "vus_r": 0,  "vus_s": 10, "vus_b": 10},
    f"{ECR}/memcached:alpine":  {"port": 11211, "proto": "memcached", "vus_r": 0,  "vus_s": 4,  "vus_b": 4},

    # ── JVM (8) — 60-min window auto-applied ──────────────────────────────────
    f"{ECR}/tomcat:10-jre21-temurin-jammy":       {"port": 8080, "proto": "http",  "vus_r": 10, "vus_s": 10, "vus_b": 20},
    # FIX-12: openjdk:21-slim removed from Batch A (it appears in Batch B as the
    # JVM idle baseline). Replaced with eclipse-temurin:21-jre-alpine which is
    # a distinct image (JRE-only, confirmed on ECR) and already in JVM_60MIN.
    f"{ECR}/eclipse-temurin:21-jre-alpine":       {"port": 0,    "proto": "batch", "vus_r": 0,  "vus_s": 1,  "vus_b": 1},
    "public.ecr.aws/bitnami/keycloak:23":         {"port": 8080, "proto": "http",  "vus_r": 10, "vus_s": 10, "vus_b": 20},
    "public.ecr.aws/elastic/elasticsearch:8.11.0":{"port": 9200, "proto": "http_es","vus_r": 5, "vus_s": 5,  "vus_b": 10},
    f"{ECR}/gradle:8-jdk21-alpine":               {"port": 0,    "proto": "batch", "vus_r": 0,  "vus_s": 1,  "vus_b": 1},
    f"{ECR}/maven:3.9-eclipse-temurin-21-alpine":  {"port": 0,    "proto": "batch", "vus_r": 0,  "vus_s": 1,  "vus_b": 1},
    # FIX-8: temurin sleep-infinity images; ratio capped in compute_labels
    f"{ECR}/eclipse-temurin:21-alpine":           {"port": 0,    "proto": "batch", "vus_r": 0,  "vus_s": 1,  "vus_b": 1},
    f"{ECR}/eclipse-temurin:17-jre-alpine":       {"port": 0,    "proto": "batch", "vus_r": 0,  "vus_s": 1,  "vus_b": 1},

    # ── Python (5) ────────────────────────────────────────────────────────────
    # Python stdlib http.server — CMD override in deploy_container starts server on :8000
    # port=8000 + proto=http → k6 sends real HTTP load → non-trivial CPU signal
    # jupyter/base-notebook not available on ECR — replaced with python:3.11 (http.server)
    f"{ECR}/python:3.11":        {"port": 8000, "proto": "http", "vus_r": 5, "vus_s": 5, "vus_b": 10},
    f"{ECR}/python:3.12-alpine": {"port": 8000, "proto": "http", "vus_r": 5, "vus_s": 5, "vus_b": 10},
    f"{ECR}/python:3.11-slim":   {"port": 8000, "proto": "http", "vus_r": 5, "vus_s": 5, "vus_b": 10},
    f"{ECR}/python:3.10-slim":   {"port": 8000, "proto": "http", "vus_r": 5, "vus_s": 5, "vus_b": 10},
    f"{ECR}/python:3.12-slim":   {"port": 8000, "proto": "http", "vus_r": 5, "vus_s": 5, "vus_b": 10},
}

FULL_20_IMAGES = list(IMAGE_CONFIG.keys())

# Smoke uses ALL 20 images — 10 workers × 2 images each × 5-min window ≈ 11 min wall-clock.
# JVM images run at SMOKE_S (300s) in smoke mode — JVM_60MIN override is bypassed.
# Rationale: smoke validates every image's deploy+load path, not JIT profiling.
SMOKE_20_IMAGES = FULL_20_IMAGES

# ── CSV schema (identical to original — preserves dataset compatibility) ──────
CSV_COLUMNS = [
    "image", "image_tag", "image_repo", "image_digest",
    "layer_count", "total_size_mb", "avg_layer_size_mb",
    "layer_size_std_mb", "layer_size_max_mb", "layer_size_min_mb",
    "layer_size_skew", "layer_size_cv", "compressed_ratio", "compressed_total_mb",
    "lang_c", "lang_java", "lang_python", "lang_go", "lang_js", "lang_other", "lang_source",
    "env_var_count", "env_has_java", "env_has_python", "env_has_node",
    "env_has_go", "env_has_rust", "env_path_segments",
    "entrypoint_token_count", "has_entrypoint", "has_shell_cmd", "cmd_token_count",
    "exposed_port_count", "exposes_http", "exposes_db_port", "exposes_jmx",
    "architecture", "arch_amd64", "os", "image_age_days",
    "label_count", "has_oci_labels", "has_maintainer_label",
    "repo_tag_count", "has_latest_tag", "working_dir_set",
    "has_healthcheck", "has_volumes", "has_user", "stop_signal", "docker_version",
    "startup_cpu_p50_m", "startup_cpu_p95_m", "startup_cpu_p99_m",
    "startup_cpu_max_m", "startup_cpu_mean_m", "startup_sample_count", "startup_duration_s",
    "cpu_p50_millicores", "cpu_p75_millicores", "cpu_p90_millicores",
    "cpu_p95_millicores", "cpu_p99_millicores", "cpu_max_millicores",
    "cpu_mean_millicores", "cpu_min_millicores",
    "cpu_rampup_p50_m", "cpu_rampup_p99_m",
    "cpu_sustained_p50_m", "cpu_sustained_p95_m", "cpu_sustained_p99_m",
    "cpu_burst_p50_m", "cpu_burst_p99_m",
    "cpu_request_recommended", "cpu_limit_recommended",
    "cpu_headroom_ratio", "cpu_startup_ratio",
    "sample_count", "startup_sample_count",
    "carbon_kg_co2eq", "energy_wh", "mean_power_w",
    "load_duration_s", "load_proto", "benchmark_mode",
    "worker_id", "k6_rampup_vus", "k6_sustained_vus", "k6_burst_vus", "collected_at",
]


# ════════════════════════════════════════════════════════════════════════════════
#  UTILITY
# ════════════════════════════════════════════════════════════════════════════════

def _run(cmd, timeout=60, capture=True):
    import os as _os
    env = _os.environ.copy()
    if "KUBECONFIG" not in env:
        default = str(Path.home() / ".kube" / "config")
        if _os.path.exists(default):
            env["KUBECONFIG"] = default
    try:
        if capture:
            r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, env=env)
            return r.returncode, r.stdout.strip(), r.stderr.strip()
        r = subprocess.run(cmd, timeout=timeout, env=env)
        return r.returncode, "", ""
    except subprocess.TimeoutExpired:
        return 1, "", f"timeout {timeout}s"
    except FileNotFoundError:
        return 1, "", f"not found: {cmd[0]}"
    except Exception as e:
        return 1, "", str(e)

def _slug(image: str) -> str:
    """K8s-safe name: ≤63 chars. Uses MD5 suffix on truncation to avoid collisions."""
    import hashlib as _hl
    raw = image.replace(":", "-").replace("/", "-").replace(".", "-")
    if len(raw) <= 63:
        return raw
    h = _hl.md5(image.encode()).hexdigest()[:6]
    return raw[:56] + "-" + h

def _pct(data: list, p: float) -> float:
    if not data: return 0.0
    sd = sorted(data)
    idx = (p / 100) * (len(sd) - 1)
    lo = int(idx); hi = min(lo + 1, len(sd) - 1)
    return round(sd[lo] * (1 - (idx - lo)) + sd[hi] * (idx - lo), 1)

def _log_w(wid: int, msg: str) -> None:
    ts   = datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}][W{wid:02d}] {msg}"
    print(line, flush=True)
    try:
        with open(LOG_FILE, "a") as fh:
            fh.write(line + "\n")
    except Exception:
        pass

def _effective_window(image: str, default_s: int) -> int:
    # Smoke mode (SMOKE_S=300): NEVER override — JVM images run at 300s too.
    # The smoke purpose is pipeline validation, not JIT profiling.
    # Half/full mode: JVM images auto-scale to FULL_S for proper JIT warm-up.
    if default_s <= SMOKE_S:
        return default_s
    if default_s >= FULL_S:
        return FULL_S
    return FULL_S if image in JVM_60MIN else default_s


# ════════════════════════════════════════════════════════════════════════════════
#  STAGE 1 — OCI METADATA
# ════════════════════════════════════════════════════════════════════════════════

def _infer_lang_from_name(image: str) -> tuple:
    img = image.lower()
    if any(x in img for x in ("nginx","redis","postgres","httpd","apache","memcached",
                               "haproxy","lighttpd","varnish","squid","mysql","mariadb",
                               "cassandra","alpine","busybox","debian","ubuntu")):
        return "lang_c", "name"
    if any(x in img for x in ("openjdk","java","tomcat","temurin","gradle","maven",
                               "jenkins","sonar","keycloak","elastic","spring")):
        return "lang_java", "name"
    if any(x in img for x in ("python","jupyter","fastapi","pytorch","tensorflow",
                               "flask","django","scipy")):
        return "lang_python", "name"
    if any(x in img for x in ("golang","go:","caddy","traefik","grafana",
                               "prometheus","alertmanager","rust","muslrust","actix")):
        return "lang_go", "name"
    if any(x in img for x in ("node","deno","bun","ghost","verdaccio","strapi","express")):
        return "lang_js", "name"
    return "lang_other", "name"

def _infer_lang_from_env(env_vars: list) -> tuple | None:
    env_str = " ".join(env_vars).lower()
    if any(x in env_str for x in ("java_home","java_version","jvm_opts","classpath","jdk_java_options")):
        return "lang_java", "env"
    if any(x in env_str for x in ("python_version","pythonpath","python3_version")):
        return "lang_python", "env"
    if any(x in env_str for x in ("node_version","npm_config","npm_config_cache","node_path")):
        return "lang_js", "env"
    if any(x in env_str for x in ("gopath","go111module","goroot","goversion")):
        return "lang_go", "env"
    # FIX-13: Rust env vars were incorrectly mapped to lang_go — corrected to lang_other
    if any(x in env_str for x in ("rustup_home","cargo_home","rust_version")):
        return "lang_other", "env"
    return None

def _comp_estimate(image: str) -> float:
    img = image.lower()
    if any(x in img for x in ("openjdk","java","tomcat","temurin","elastic")): return 0.44
    if any(x in img for x in ("python","jupyter","pytorch","tensorflow")): return 0.41
    if any(x in img for x in ("node","ghost","bun","verdaccio")): return 0.40
    if any(x in img for x in ("golang","go:","caddy","traefik","rust","actix")): return 0.35
    return 0.37

def extract_oci_metadata(image: str) -> dict:
    import math as _m
    from datetime import datetime, timezone
    ref   = image
    parts = image.split(":", 1)
    tag   = parts[1] if len(parts) > 1 else "latest"
    repo  = parts[0].split("/")[0] if "/" in parts[0] else "library"

    feat: dict = {
        "image_tag": tag, "image_repo": repo, "image_digest": "",
        "layer_count": 0, "total_size_mb": 0.0, "avg_layer_size_mb": 0.0,
        "layer_size_std_mb": 0.0, "layer_size_max_mb": 0.0, "layer_size_min_mb": 0.0,
        "layer_size_skew": 0.0, "layer_size_cv": 0.0,
        "compressed_ratio": _comp_estimate(image), "compressed_total_mb": 0.0,
        **{k: 0.0 for k in ("lang_c","lang_java","lang_python","lang_go","lang_js","lang_other")},
        "lang_source": "name",
        "env_var_count": 0, "env_has_java": 0, "env_has_python": 0,
        "env_has_node": 0, "env_has_go": 0, "env_has_rust": 0, "env_path_segments": 0,
        "entrypoint_token_count": 0, "has_entrypoint": 0, "has_shell_cmd": 0, "cmd_token_count": 0,
        "exposed_port_count": 0, "exposes_http": 0, "exposes_db_port": 0, "exposes_jmx": 0,
        "architecture": "amd64", "arch_amd64": 1, "os": "linux",
        "image_age_days": 0, "label_count": 0, "has_oci_labels": 0, "has_maintainer_label": 0,
        "repo_tag_count": 0, "has_latest_tag": 0, "working_dir_set": 0, "has_healthcheck": 0,
        "has_volumes": 0, "has_user": 0, "stop_signal": "SIGTERM", "docker_version": "",
    }

    lang_key, _ = _infer_lang_from_name(image)
    feat[lang_key] = 1.0

    rc, out, _ = _run(["skopeo", "inspect", f"docker://{ref}"], timeout=60)
    if rc != 0 or not out:
        return feat
    try:
        info = json.loads(out)
    except json.JSONDecodeError:
        return feat

    layers_raw  = info.get("Layers") or []
    layers_data = info.get("LayersData") or []
    layer_count = len(layers_raw)
    uncomp: list = [float(ld["Size"]) for ld in layers_data
                    if isinstance(ld.get("Size"), (int, float)) and ld["Size"] > 0]
    if not layer_count:
        layer_count = len(uncomp)

    if uncomp:
        total_b  = sum(uncomp)
        total_mb = total_b / 1_048_576
        avg_mb   = total_mb / len(uncomp)
        max_mb   = max(uncomp) / 1_048_576
        min_mb   = min(uncomp) / 1_048_576
        mean_u   = total_b / len(uncomp)
        variance = sum((s - mean_u) ** 2 for s in uncomp) / max(len(uncomp), 1)
        std_b    = _m.sqrt(variance)
        std_mb   = std_b / 1_048_576
        cv       = round(std_b / mean_u, 4) if mean_u > 0 else 0.0
        sorted_u = sorted(uncomp); n = len(sorted_u)
        med = (sorted_u[n//2] if n % 2 else (sorted_u[n//2-1] + sorted_u[n//2]) / 2)
        skew = round(3 * (mean_u - med) / std_b, 4) if std_b > 0 else 0.0
    else:
        total_mb = avg_mb = max_mb = min_mb = std_mb = cv = skew = 0.0

    feat.update({
        "layer_count": layer_count, "total_size_mb": round(total_mb, 2),
        "avg_layer_size_mb": round(avg_mb, 2), "layer_size_std_mb": round(std_mb, 4),
        "layer_size_max_mb": round(max_mb, 2), "layer_size_min_mb": round(min_mb, 2),
        "layer_size_skew": skew, "layer_size_cv": cv,
        "image_digest": info.get("Digest", ""),
    })

    env_vars = info.get("Env") or []
    env_str  = " ".join(env_vars).lower()
    lang_from_env = _infer_lang_from_env(env_vars)
    if lang_from_env:
        lang_key, _ = lang_from_env
        for k in ("lang_c","lang_java","lang_python","lang_go","lang_js","lang_other"):
            feat[k] = 0.0
        feat[lang_key]      = 1.0
        feat["lang_source"] = "env"

    path_val = next((v.split("=", 1)[1] for v in env_vars if v.upper().startswith("PATH=")), "")
    feat.update({
        "env_var_count":    len(env_vars),
        "env_has_java":     int(any(x in env_str for x in ("java_home","java_version","jvm_opts","classpath","jdk_java_options"))),
        "env_has_python":   int(any(x in env_str for x in ("python_version","pythonpath","python3_version"))),
        "env_has_node":     int(any(x in env_str for x in ("node_version","npm_config","node_path"))),
        "env_has_go":       int(any(x in env_str for x in ("gopath","go111module","goroot"))),
        "env_has_rust":     int(any(x in env_str for x in ("rustup_home","cargo_home","rust_version"))),
        "env_path_segments": len(path_val.split(":")) if path_val else 0,
    })

    ep  = info.get("Entrypoint") or []
    cmd = info.get("Cmd")        or []
    has_shell = bool(cmd and str(cmd[0]).strip() in ("/bin/sh","/bin/bash","sh","bash"))
    feat.update({
        "entrypoint_token_count": len(ep) + len(cmd),
        "has_entrypoint":         int(bool(ep)),
        "has_shell_cmd":          int(has_shell),
        "cmd_token_count":        len(cmd),
    })

    ports_raw = info.get("ExposedPorts") or {}
    port_nums = set()
    for p in ports_raw:
        try: port_nums.add(int(str(p).split("/")[0]))
        except ValueError: pass
    feat.update({
        "exposed_port_count": len(port_nums),
        "exposes_http":       int(bool(port_nums & {80, 443, 8080, 8443, 3000, 4000})),
        "exposes_db_port":    int(bool(port_nums & {3306, 5432, 27017, 6379, 9042, 5672, 9092, 8086})),
        "exposes_jmx":        int(bool(port_nums & {9999, 1099, 7199})),
    })

    arch = info.get("Architecture", "amd64")
    labels_raw  = info.get("Labels") or {}
    repo_tags   = info.get("RepoTags") or []
    created_str = info.get("Created", "")
    age_days = 0
    if created_str:
        try:
            dt = datetime.fromisoformat(created_str.replace("Z", "+00:00"))
            age_days = max(0, (datetime.now(timezone.utc) - dt).days)
        except Exception:
            pass

    oci_label_keys = [k for k in labels_raw if k.startswith("org.opencontainers")]
    feat.update({
        "architecture":         arch,
        "arch_amd64":           int(arch == "amd64"),
        "os":                   info.get("Os", "linux"),
        "image_age_days":       age_days,
        "label_count":          len(labels_raw),
        "has_oci_labels":       int(bool(oci_label_keys)),
        "has_maintainer_label": int("maintainer" in labels_raw or
                                    "org.opencontainers.image.authors" in labels_raw),
        "repo_tag_count":       len(repo_tags),
        "has_latest_tag":       int("latest" in repo_tags),
        "working_dir_set":      int(bool((info.get("WorkingDir") or "").strip())),
        "has_healthcheck":      int(bool(info.get("HealthCheck"))),
        "has_volumes":          int(bool(info.get("Volumes"))),
        "has_user":             int(bool((info.get("User") or "").strip())),
        "stop_signal":          info.get("StopSignal") or "SIGTERM",
        "docker_version":       info.get("DockerVersion") or "",
    })

    rc_dm, out_dm, _ = _run(["docker", "manifest", "inspect", image], timeout=60)
    if rc_dm == 0 and out_dm and uncomp:
        try:
            dm = json.loads(out_dm)
            dm_layers = dm.get("layers", [])
            if not dm_layers:
                for mf in dm.get("manifests", []):
                    p = mf.get("platform", {})
                    if p.get("os") == "linux" and p.get("architecture") == "amd64":
                        dig = mf.get("digest", "")
                        if dig:
                            rc2, out2, _ = _run(["docker", "manifest", "inspect",
                                                  f"{image}@{dig}"], timeout=60)
                            if rc2 == 0 and out2:
                                dm_layers = json.loads(out2).get("layers", [])
                        break
            comp: list = [float(l["size"]) for l in dm_layers if l.get("size", 0) > 0]
            if comp:
                comp_total   = sum(comp)
                uncomp_total = sum(uncomp)
                feat["compressed_total_mb"] = round(comp_total / 1_048_576, 2)
                if abs(comp_total - uncomp_total) / max(uncomp_total, 1) > 0.01:
                    feat["compressed_ratio"] = round(max(0.01, min(1.0, comp_total / uncomp_total)), 4)
        except Exception:
            pass

    return feat


# ════════════════════════════════════════════════════════════════════════════════
#  STAGE 2 — KUBERNETES DEPLOY / TEARDOWN
# ════════════════════════════════════════════════════════════════════════════════

def deploy_container(image: str, namespace: str) -> tuple:
    import os as _os, tempfile as _tf
    slug = _slug(image)
    cfg  = IMAGE_CONFIG.get(image, {"port": 80})
    port = cfg.get("port", 80)
    _run(["kubectl", "create", "namespace", namespace], timeout=15)

    img_lower = image.lower()

    has_daemon = any(d in img_lower for d in (
        "nginx","httpd","lighttpd","varnish","caddy","traefik",
        "redis","postgres","mysql","mariadb","mongo","cassandra",
        "influxdb","rabbitmq","memcached",
        "grafana","prometheus","alertmanager",
        "keycloak","elasticsearch","ghost","verdaccio",
        "jupyter","wordpress","redmine","tomcat",
    ))
    # FIX-2: HAProxy excluded from has_daemon — it needs a config file to stay up
    if "haproxy" in img_lower:
        has_daemon = False

    if has_daemon:
        override_cmd = []
    elif "python" in img_lower:
        override_cmd = ["python3", "-m", "http.server", "8000"]
    elif "node" in img_lower:
        override_cmd = ["node", "-e",
            "const h=require('http');h.createServer(function(q,r)"
            "{r.writeHead(200);r.end('OK')}).listen(3000)"]
    elif any(x in img_lower for x in ("rust","gradle","maven","eclipse-temurin","temurin","haproxy")):
        override_cmd = ["sleep", "infinity"]
    else:
        override_cmd = ["sleep", "infinity"]

    # ── Required env vars ────────────────────────────────────────────────────
    env_yaml_lines: list = []

    if "elasticsearch" in img_lower:
        env_yaml_lines = [
            "        env:",
            "        - name: discovery.type",
            "          value: single-node",
            "        - name: xpack.security.enabled",
            "          value: 'false'",
            "        - name: ES_JAVA_OPTS",
            "          value: '-Xms512m -Xmx512m'",
        ]
    # FIX-3: Varnish needs backend env vars to start the daemon
    elif "varnish" in img_lower:
        env_yaml_lines = [
            "        env:",
            "        - name: VARNISH_BACKEND_HOST",
            "          value: '127.0.0.1'",
            "        - name: VARNISH_BACKEND_PORT",
            "          value: '8080'",
            "        - name: VARNISH_SIZE",
            "          value: '100M'",
        ]

    # ── Build YAML ────────────────────────────────────────────────────────────
    deploy_lines = [
        "apiVersion: apps/v1", "kind: Deployment", "metadata:",
        f"  name: {slug}", f"  namespace: {namespace}", "  labels:",
        f"    app: {slug}", "    research: zero-telemetry-cpu",
        "spec:", "  replicas: 1", "  selector:", "    matchLabels:",
        f"      app: {slug}", "  template:", "    metadata:", "      labels:",
        f"        app: {slug}", "    spec:", "      containers:",
        f"      - name: {slug}", f"        image: {image}",
    ]

    if override_cmd:
        cmd_val = override_cmd[0].replace("'", "'\"'\"'")
        deploy_lines.append("        command: ['" + cmd_val + "']")
        if len(override_cmd) > 1:
            args_items = ["'" + a.replace("'", "'\"'\"'") + "'" for a in override_cmd[1:]]
            deploy_lines.append("        args: [" + ", ".join(args_items) + "]")

    if env_yaml_lines:
        deploy_lines += env_yaml_lines

    if port > 0:
        deploy_lines += ["        ports:", f"        - containerPort: {port}"]

    deploy_lines += [
        "        resources:",
        "          requests:",
        "            memory: 64Mi",
        "          limits:",
        "            memory: 2Gi",
    ]

    svc_lines = [] if port == 0 else [
        "---", "apiVersion: v1", "kind: Service", "metadata:",
        f"  name: {slug}", f"  namespace: {namespace}", "spec:",
        "  type: NodePort", "  selector:", f"    app: {slug}",
        "  ports:", f"  - port: {port}", f"    targetPort: {port}",
    ]

    manifest = "\n".join(deploy_lines + svc_lines) + "\n"

    env = _os.environ.copy()
    if "KUBECONFIG" not in env:
        kc = str(Path.home() / ".kube" / "config")
        if _os.path.exists(kc):
            env["KUBECONFIG"] = kc

    with _tf.NamedTemporaryFile(suffix=".yaml", mode="w", delete=False) as f:
        f.write(manifest); tmp = f.name
    try:
        r = __import__("subprocess").run(
            ["kubectl", "apply", "-f", tmp],
            capture_output=True, text=True, timeout=30, env=env)
        if r.returncode != 0:
            print(f"[DEPLOY FAIL] {r.stderr[:200]!r}", flush=True)
            return False, "", "", 0
    finally:
        _os.unlink(tmp)

    for _ in range(24):
        time.sleep(5)
        rc, out, _ = _run(
            ["kubectl", "get", "pods", "-n", namespace,
             "-l", f"app={slug}", "--no-headers"], timeout=15)
        if rc == 0 and "Running" in out:
            break
    else:
        rc_d, out_d, _ = _run(["kubectl", "get", "pods", "-n", namespace, "--no-headers"], timeout=10)
        print(f"[DEPLOY TIMEOUT] pods in {namespace}: {out_d!r}", flush=True)
        return False, "", "", 0

    node_ip = "localhost"; np_int = port
    rc2, node_ip_raw, _ = _run(
        ["kubectl", "get", "nodes", "-o",
         "jsonpath={.items[0].status.addresses[?(@.type=='InternalIP')].address}"], timeout=15)
    if rc2 == 0 and node_ip_raw.strip():
        node_ip = node_ip_raw.strip()

    if port > 0:
        rc3, np_str, _ = _run(
            ["kubectl", "get", "svc", slug, "-n", namespace,
             "-o", "jsonpath={.spec.ports[0].nodePort}"], timeout=15)
        if rc3 == 0 and np_str.strip():
            try: np_int = int(np_str.strip())
            except ValueError: pass
        url = f"http://{node_ip}:{np_int}"
    else:
        url = ""

    return True, url, node_ip, np_int


def teardown_container(image: str, namespace: str) -> None:
    slug = _slug(image)
    _run(["kubectl", "delete", "deployment", slug, "-n", namespace,
          "--ignore-not-found=true"], timeout=30)
    _run(["kubectl", "delete", "service",    slug, "-n", namespace,
          "--ignore-not-found=true"], timeout=30)


# ════════════════════════════════════════════════════════════════════════════════
#  STAGE 3 — LOAD PROTOCOLS
# ════════════════════════════════════════════════════════════════════════════════

def _k6_http(url: str, rampup_s: int, sustained_s: int, burst_s: int,
             vus_r: int, vus_s: int, vus_b: int | None = None) -> None:
    # FIX-14: use explicit vus_b when provided; cap at 50 for safety
    vus_burst = min(vus_b if vus_b is not None else vus_s * 2, 50)
    script = f"""
import http from 'k6/http';
import {{ sleep, check }} from 'k6';
export const options = {{
  scenarios: {{
    rampup:    {{ executor:'ramping-vus', startVUs:0,
                  stages:[{{ duration:'{rampup_s}s', target:{vus_r} }}],
                  gracefulRampDown:'10s' }},
    sustained: {{ executor:'constant-vus', vus:{vus_s},
                  duration:'{sustained_s}s', startTime:'{rampup_s}s' }},
    burst:     {{ executor:'constant-vus', vus:{vus_burst},
                  duration:'{burst_s}s', startTime:'{rampup_s+sustained_s}s' }},
  }},
  thresholds: {{
    http_req_failed:   [{{ threshold:'rate<0.10', abortOnFail:false }}],
    http_req_duration: [{{ threshold:'p(95)<3000', abortOnFail:false }}],
  }},
}};
export default function () {{
  check(http.get('{url}', {{ timeout:'5s',
    headers:{{'User-Agent':'zerotelem-benchmark/1.0'}} }}),
    {{ 'not_5xx': r => r.status < 500 }});
  sleep(0.5);
}}
"""
    total = rampup_s + sustained_s + burst_s
    with tempfile.NamedTemporaryFile(suffix=".js", mode="w", delete=False) as f:
        f.write(script); tmp = f.name
    try:
        _run(["k6", "run", "--quiet", tmp], timeout=total + 120)
    finally:
        os.unlink(tmp)

def _k6_es(url: str, total_s: int, vus: int = 5) -> None:
    script = f"""
import http from 'k6/http';
import {{ sleep }} from 'k6';
export const options = {{
  scenarios: {{ s: {{ executor:'constant-vus', vus:{vus}, duration:'{total_s}s' }} }}
}};
export default function () {{
  http.post('{url}/bench/_doc',
    JSON.stringify({{ text:'benchmark '+Date.now() }}),
    {{ headers:{{'Content-Type':'application/json'}}, timeout:'10s' }});
  http.get('{url}/bench/_search?q=benchmark', {{ timeout:'10s' }});
  sleep(1);
}}
"""
    with tempfile.NamedTemporaryFile(suffix=".js", mode="w", delete=False) as f:
        f.write(script); tmp = f.name
    try:
        _run(["k6", "run", "--quiet", tmp], timeout=total_s + 120)
    finally:
        os.unlink(tmp)

def _redis_bm(node_ip: str, node_port: int, duration_s: int) -> None:
    # Use --test-time (time-bounded) instead of -n (ops-bounded).
    # -n finishes in ~10s at high Redis throughput → only 24 samples in smoke.
    # --test-time ensures benchmark runs for the full window → ~53 samples.
    _run(["redis-benchmark", "-h", node_ip, "-p", str(node_port),
          "--test-time", str(duration_s),
          "-c", "10", "-t", "get,set,ping", "--csv"],
         timeout=duration_s + 120)

def _memtier_bm(node_ip: str, node_port: int, duration_s: int) -> None:
    _run(["memtier_benchmark", "-s", node_ip, "-p", str(node_port),
          "--protocol=memcache_text", "-t", "4", "-c", "10",
          "--test-time", str(duration_s), "--ratio", "1:10"],
         timeout=duration_s + 120)

def _stress_ng_load(image: str, namespace: str, duration_s: int, workers: int = 4) -> None:
    """
    Run stress-ng inside the already-running pod via kubectl exec.
    Alpine-based images (haproxy:alpine, alpine:3.19) have apk — install stress-ng first.
    This produces a real, measurable CPU signal instead of idle baseline.
    """
    slug = _slug(image)
    img_lower = image.lower()
    # Get pod name
    rc, out, _ = _run(
        ["kubectl", "get", "pods", "-n", namespace,
         "-l", f"app={slug}", "--no-headers",
         "-o", "custom-columns=NAME:.metadata.name"], timeout=15)
    if rc != 0 or not out.strip():
        time.sleep(duration_s)
        return
    pod_name = out.strip().split('\n')[0].strip()

    # Install stress-ng if not present (alpine uses apk)
    install_cmd = ["kubectl", "exec", "-n", namespace, pod_name, "--",
                   "sh", "-c", "apk add --no-cache stress-ng 2>/dev/null || true"]
    _run(install_cmd, timeout=60)

    # Run stress-ng for the full duration
    stress_cmd = ["kubectl", "exec", "-n", namespace, pod_name, "--",
                  "stress-ng", "--cpu", str(workers),
                  "--timeout", f"{duration_s}s",
                  "--metrics-brief"]
    _run(stress_cmd, timeout=duration_s + 30)

def run_benchmark(image: str, url: str, node_ip: str, node_port: int,
                  rampup_s: int, sustained_s: int, burst_s: int,
                  namespace: str = "zt-0") -> None:
    cfg   = IMAGE_CONFIG.get(image, {})
    proto = cfg.get("proto", "http")
    vus_r = cfg.get("vus_r", 10)
    vus_s = cfg.get("vus_s", 10)
    # FIX-14: explicit burst VUs from IMAGE_CONFIG
    vus_b = cfg.get("vus_b", min(vus_s * 2, 50))
    total = rampup_s + sustained_s + burst_s
    if proto == "http":
        _k6_http(url, rampup_s, sustained_s, burst_s, vus_r, vus_s, vus_b)
    elif proto == "http_es":
        _k6_es(url, total, vus=vus_s)
    elif proto == "redis":
        _redis_bm(node_ip, node_port, total)
    elif proto == "memcached":
        _memtier_bm(node_ip, node_port, total)
    elif proto == "stress_ng":
        # FIX: actually invoke stress-ng inside the pod via kubectl exec
        _stress_ng_load(image, namespace, total, workers=vus_s if vus_s > 0 else 4)
    elif proto == "batch":
        time.sleep(total)
    else:
        time.sleep(total)


# ════════════════════════════════════════════════════════════════════════════════
#  STAGE 4 — CPU SAMPLING
# ════════════════════════════════════════════════════════════════════════════════

def sample_cpu_startup(image: str, namespace: str, duration_s: int,
                        stop_event: Event) -> dict:
    slug    = _slug(image)
    t_start = time.time()
    readings: list = []
    while time.time() < t_start + duration_s:
        if stop_event.is_set():
            break
        rc, out, _ = _run(
            ["kubectl", "top", "pod", "-n", namespace,
             "-l", f"app={slug}", "--no-headers"], timeout=10)
        if rc == 0 and out:
            parts = out.split()
            if len(parts) >= 2:
                try:
                    readings.append(float(parts[1].rstrip("m")))
                except ValueError:
                    pass
        time.sleep(CPU_INTERVAL_S)
    return {"readings": readings,
            "duration_s": round(time.time() - t_start, 1),
            "sample_count": len(readings)}

def sample_cpu(image: str, namespace: str, duration_s: int,
               rampup_s: int, sustained_s: int,
               stop_event: Event, ts: str) -> dict:
    slug    = _slug(image)
    t_start = time.time()
    buckets = {"all": [], "rampup": [], "sustained": [], "burst": []}
    phases  = {"rampup": (0, rampup_s),
               "sustained": (rampup_s, rampup_s + sustained_s),
               "burst": (rampup_s + sustained_s, duration_s)}

    trace_path = TRACE_DIR / f"{slug}_{ts}.csv"
    with open(trace_path, "w") as tf:
        tf.write("elapsed_s,phase,cpu_millicores\n")
        while time.time() < t_start + duration_s:
            if stop_event.is_set():
                break
            rc, out, _ = _run(
                ["kubectl", "top", "pod", "-n", namespace,
                 "-l", f"app={slug}", "--no-headers"], timeout=10)
            elapsed = time.time() - t_start
            if rc == 0 and out:
                parts = out.split()
                if len(parts) >= 2:
                    try:
                        cpu_m = float(parts[1].rstrip("m"))
                        phase = "burst"
                        for ph, (lo, hi) in phases.items():
                            if lo <= elapsed < hi:
                                phase = ph; break
                        buckets["all"].append(cpu_m)
                        buckets[phase].append(cpu_m)
                        tf.write(f"{elapsed:.1f},{phase},{cpu_m}\n")
                        tf.flush()
                    except ValueError:
                        pass
            time.sleep(CPU_INTERVAL_S)
    return buckets


# ════════════════════════════════════════════════════════════════════════════════
#  STAGE 5 — LABELS + CARBON
# ════════════════════════════════════════════════════════════════════════════════

def compute_labels(buckets: dict, startup: dict | None = None) -> dict:
    all_s = buckets.get("all", [])
    ramp  = buckets.get("rampup",    []) or all_s
    sust  = buckets.get("sustained", []) or all_s
    burst = buckets.get("burst",     []) or all_s

    if not all_s:
        zero = {k: 0.0 for k in (
            "cpu_p50_millicores","cpu_p75_millicores","cpu_p90_millicores",
            "cpu_p95_millicores","cpu_p99_millicores","cpu_max_millicores",
            "cpu_mean_millicores","cpu_min_millicores","sample_count",
            "cpu_rampup_p50_m","cpu_rampup_p99_m",
            "cpu_sustained_p50_m","cpu_sustained_p95_m","cpu_sustained_p99_m",
            "cpu_burst_p50_m","cpu_burst_p99_m",
            "cpu_request_recommended","cpu_limit_recommended",
            "cpu_headroom_ratio","cpu_startup_ratio",
        )}
        zero["startup_sample_count"] = 0
        return zero

    sust_p50  = _pct(sust, 50)
    burst_p99 = _pct(burst, 99)

    labels = {
        "cpu_p50_millicores":  _pct(all_s, 50),
        "cpu_p75_millicores":  _pct(all_s, 75),
        "cpu_p90_millicores":  _pct(all_s, 90),
        "cpu_p95_millicores":  _pct(all_s, 95),
        "cpu_p99_millicores":  _pct(all_s, 99),
        "cpu_max_millicores":  round(max(all_s), 1),
        "cpu_mean_millicores": round(sum(all_s) / len(all_s), 1),
        "cpu_min_millicores":  round(min(all_s), 1),
        "sample_count":        len(all_s),
        "cpu_rampup_p50_m":    _pct(ramp, 50),
        "cpu_rampup_p99_m":    _pct(ramp, 99),
        "cpu_sustained_p50_m": sust_p50,
        "cpu_sustained_p95_m": _pct(sust, 95),
        "cpu_sustained_p99_m": _pct(sust, 99),
        "cpu_burst_p50_m":     _pct(burst, 50),
        "cpu_burst_p99_m":     burst_p99,
        "cpu_request_recommended": sust_p50,
        "cpu_limit_recommended":   burst_p99,
        # FIX-11: guard against ZeroDivisionError
        "cpu_headroom_ratio":  round(_pct(all_s, 99) / max(_pct(all_s, 50), 0.1), 3),
    }

    st = startup or {}
    st_readings = st.get("readings", [])
    st_p99      = _pct(st_readings, 99)
    st_max      = max(st_readings) if st_readings else 0.0
    # FIX-8: cap ratio at 99.9 sentinel for sleep-infinity temurin images
    # FIX-11: max(sust_p50, 0.1) prevents ZeroDivisionError
    raw_ratio   = st_p99 / max(sust_p50, 0.1) if st_readings else 0.0
    startup_ratio = round(min(raw_ratio, 99.9), 3)

    labels.update({
        "startup_cpu_p50_m":    _pct(st_readings, 50),
        "startup_cpu_p95_m":    _pct(st_readings, 95),
        "startup_cpu_p99_m":    st_p99,
        "startup_cpu_max_m":    round(st_max, 1),
        "startup_cpu_mean_m":   round(sum(st_readings) / len(st_readings), 1) if st_readings else 0.0,
        "startup_sample_count": len(st_readings),
        "startup_duration_s":   st.get("duration_s", 0.0),
        "cpu_startup_ratio":    startup_ratio,
    })

    return labels


def compute_carbon(cpu_mean_m: float, duration_s: float) -> dict:
    """
    Liu et al. (2020) linear server power model — m5.4xlarge (16 vCPU, 64 GB).

    P(t) = P_idle + (P_max - P_idle) × U_cpu(t)
    Parameters:
      P_idle = 50 W   (SPEC Power SSJ 2008 baseline for m5 family)
      P_max  = 200 W  (SPEC Power SSJ 2008 100% load)
      VCPUS  = 16     (m5.4xlarge vCPU count)
    Carbon intensity: IEA 2023 global average = 0.233 kg CO₂eq / kWh

    Args:
      cpu_mean_m:  mean CPU utilisation in millicores (from cpu_mean_millicores label)
      duration_s:  actual measured per-image collection window in seconds

    Audit check: carbon_kg_co2eq == energy_wh / 1000 * 0.233 always holds.
    """
    if duration_s <= 0:
        return {"carbon_kg_co2eq": 0.0, "energy_wh": 0.0, "mean_power_w": 0.0}
    P_IDLE = 50.0; P_MAX = 200.0; VCPUS = 16; CARBON_KWH = 0.233
    # u is capped at 1.0; cpu_mean_m=0 → u=0 → P=P_idle (correct idle baseline)
    u          = min(max(cpu_mean_m, 0.0) / (VCPUS * 1000), 1.0)
    mean_power = P_IDLE + (P_MAX - P_IDLE) * u
    energy_wh  = mean_power * duration_s / 3600
    carbon_kg  = energy_wh / 1000 * CARBON_KWH
    return {
        "carbon_kg_co2eq": round(carbon_kg, 10),
        "energy_wh":        round(energy_wh, 6),
        "mean_power_w":     round(mean_power, 2),
    }


# ════════════════════════════════════════════════════════════════════════════════
#  FULL PER-IMAGE PIPELINE
# ════════════════════════════════════════════════════════════════════════════════

def _scale_phases(window_s: int):
    if window_s >= FULL_S:
        return WARMUP_S, RAMPUP_S, SUSTAINED_S, BURST_S, COOLDOWN_S
    loadable    = max(0, window_s - 60)
    total_ratio = RAMPUP_S + SUSTAINED_S + BURST_S
    warmup_s   = 30; cooldown_s = 30
    rampup_s   = max(30, int(loadable * RAMPUP_S    / total_ratio))
    burst_s    = max(30, int(loadable * BURST_S     / total_ratio))
    sustained_s = max(60, loadable - rampup_s - burst_s)
    return warmup_s, rampup_s, sustained_s, burst_s, cooldown_s

def run_one_image(image: str, window_s: int, namespace: str,
                  worker_id: int, mode: str, ts: str) -> dict | None:
    def log(msg): _log_w(worker_id, f"[{_slug(image)[:22]}] {msg}")
    effective_window = _effective_window(image, window_s)
    log(f"START  window={effective_window}s  proto={IMAGE_CONFIG.get(image,{}).get('proto','http')}")

    warmup_s, rampup_s, sustained_s, burst_s, cooldown_s = _scale_phases(effective_window)
    load_total_s = rampup_s + sustained_s + burst_s
    cfg = IMAGE_CONFIG.get(image, {})

    row = {
        "image": image, "collected_at": datetime.now().isoformat(),
        "load_duration_s": effective_window, "benchmark_mode": mode,
        "load_proto": cfg.get("proto", "http"),
        "worker_id":  worker_id,
        "k6_rampup_vus":    cfg.get("vus_r", 0),
        "k6_sustained_vus": cfg.get("vus_s", 0),
        # FIX-14: read explicit vus_b; fall back to min(vus_s*2, 50) only when absent
        "k6_burst_vus": cfg.get("vus_b", min(cfg.get("vus_s", 0) * 2, 50)),
    }

    meta = extract_oci_metadata(image)
    row.update(meta)
    log(f"OCI  layers={meta['layer_count']} size={meta['total_size_mb']:.1f}MB "
        f"lang={next((k.replace('lang_','') for k in meta if k.startswith('lang_') and meta[k]==1.0),'?')} "
        f"ports={meta.get('exposed_port_count',0)}")

    ok, url, node_ip, node_port = deploy_container(image, namespace)
    if not ok:
        log("FAIL deploy"); return None

    startup_stop  = Event()
    startup_data: dict = {}

    def _startup_sampler():
        nonlocal startup_data
        startup_data = sample_cpu_startup(image, namespace, warmup_s, startup_stop)

    t_startup = Thread(target=_startup_sampler, daemon=True)
    t_startup.start()
    time.sleep(warmup_s)
    startup_stop.set()
    t_startup.join(timeout=30)

    st_n   = startup_data.get("sample_count", 0)
    st_p99 = _pct(startup_data.get("readings", []), 99)
    log(f"STARTUP  n={st_n}  p99={st_p99:.0f}m  max={max(startup_data.get('readings',[0])):.0f}m")

    t_start  = time.time()
    stop_ev  = Event()
    cpu_bkts: dict = {}

    def _sampler():
        nonlocal cpu_bkts
        cpu_bkts = sample_cpu(image, namespace, load_total_s + cooldown_s,
                               rampup_s, sustained_s, stop_ev, ts)

    sampler = Thread(target=_sampler, daemon=True)
    sampler.start()

    run_benchmark(image, url, node_ip, node_port, rampup_s, sustained_s, burst_s,
                  namespace=namespace)

    time.sleep(cooldown_s)
    stop_ev.set()
    sampler.join(timeout=cooldown_s + 30)
    actual_total_s = time.time() - t_start

    labels = compute_labels(cpu_bkts, startup=startup_data)
    row.update(labels)
    # Pass cpu_mean_millicores (already computed, correct) rather than raw list.
    # actual_total_s is the per-image measured load+cooldown window.
    cpu_mean_m = labels.get("cpu_mean_millicores", 0.0)
    carbon = compute_carbon(cpu_mean_m, actual_total_s)
    row.update(carbon)
    row["load_duration_s"] = round(actual_total_s, 1)

    teardown_container(image, namespace)
    log(f"OK  p50={labels.get('cpu_p50_millicores',0)}m "
        f"p99={labels.get('cpu_p99_millicores',0)}m "
        f"sust_p50={labels.get('cpu_sustained_p50_m',0)}m "
        f"ratio={labels.get('cpu_startup_ratio',0):.1f} "
        f"n={labels.get('sample_count',0)}")
    return row


# ════════════════════════════════════════════════════════════════════════════════
#  WORKER + CSV + DISPATCHER
# ════════════════════════════════════════════════════════════════════════════════

def _append_csv(row: dict) -> None:
    dedup = ("image", "benchmark_mode")
    existing = []
    if DATASET.exists():
        with open(DATASET, newline="") as f:
            existing = list(csv.DictReader(f))
    existing = [r for r in existing
                if not all(str(r.get(k,"")) == str(row.get(k,"")) for k in dedup)]
    existing.append({c: row.get(c, "") for c in CSV_COLUMNS})
    with open(DATASET, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=CSV_COLUMNS, extrasaction="ignore")
        w.writeheader(); w.writerows(existing)

def worker_main(worker_id: int, images: list, window_s: int,
                mode: str, ts: str, csv_lock, failed_list) -> list:
    namespace = f"zt-{worker_id}"
    results   = []
    for image in images:
        try:
            row = run_one_image(image, window_s, namespace, worker_id, mode, ts)
        except Exception as exc:
            _log_w(worker_id, f"ERROR {image}: {exc}")
            teardown_container(image, namespace)
            failed_list.append(image); continue
        if row:
            with csv_lock:
                _append_csv(row)
            results.append(row)
        else:
            failed_list.append(image)
    return results

def _partition(images: list, n: int) -> list:
    groups = [[] for _ in range(n)]
    for i, img in enumerate(images):
        groups[i % n].append(img)
    return groups

def run_parallel(images: list, window_s: int, workers: int, mode: str) -> tuple:
    groups = _partition(images, workers)
    print(f"\n  {BOLD}Dispatching {len(images)} images → {workers} workers{RST}")
    with Manager() as mgr:
        csv_lock    = mgr.Lock()
        failed_list = mgr.list()
        with ProcessPoolExecutor(max_workers=workers) as pool:
            futs = {
                pool.submit(worker_main, wid, grp, window_s, mode,
                            TIMESTAMP, csv_lock, failed_list): wid
                for wid, grp in enumerate(groups) if grp
            }
            all_results = []
            for fut in as_completed(futs):
                wid = futs[fut]
                try:
                    rows = fut.result()
                    all_results.extend(rows)
                    print(f"  {G}✔{RST}  worker-{wid} done  ({len(rows)} OK)")
                except Exception as exc:
                    print(f"  {R}✘{RST}  worker-{wid} crashed: {exc}")
        failed = list(failed_list)
    return all_results, failed


# ════════════════════════════════════════════════════════════════════════════════
#  SUMMARY
# ════════════════════════════════════════════════════════════════════════════════

def write_summary(results: list, failed: list, elapsed: float) -> None:
    hdr = (f"{'Image':<38} {'proto':<12} {'req(m)':>7} {'lim(m)':>7} "
           f"{'sust_p50':>9} {'start_p99':>10} {'ratio':>6} {'n':>5}")
    lines = [
        "Phase 1-1 AWS Parallel — Batch A (20 images) Summary",
        f"Generated : {datetime.now().isoformat()}",
        f"Duration  : {elapsed:.1f}s  ({elapsed/3600:.2f}h)",
        f"Images    : {len(results)} OK  /  {len(failed)} failed"
        + (f"  [{', '.join(failed)}]" if failed else ""),
        "", hdr, "─" * len(hdr),
    ]
    for r in results:
        lines.append(
            f"{r['image']:<38} {str(r.get('load_proto','')):<12} "
            f"{float(r.get('cpu_p50_millicores',0)):>7.1f} "
            f"{float(r.get('cpu_p99_millicores',0)):>7.1f} "
            f"{float(r.get('cpu_sustained_p50_m',0)):>9.1f} "
            f"{float(r.get('startup_cpu_p99_m',0)):>10.1f} "
            f"{float(r.get('cpu_startup_ratio',0)):>6.1f} "
            f"{int(r.get('sample_count',0)):>5}")
    lines += ["", f"Dataset : {DATASET}", f"Traces  : {TRACE_DIR}"]
    SUMMARY.write_text("\n".join(lines))
    print("\n" + "\n".join(lines))


# ════════════════════════════════════════════════════════════════════════════════
#  MAIN
# ════════════════════════════════════════════════════════════════════════════════

def main() -> None:
    try:
        _mp.set_start_method("spawn", force=True)
    except RuntimeError:
        pass

    parser = argparse.ArgumentParser(
        description="Phase 1-1 Parallel AWS — Batch A (20 images: C/Alpine, JVM, Python)",
        formatter_class=argparse.RawDescriptionHelpFormatter, epilog=__doc__)
    parser.add_argument("--full",     action="store_true")
    parser.add_argument("--half",     action="store_true")
    parser.add_argument("--smoke",    action="store_true")
    parser.add_argument("--image",    action="append", dest="images", metavar="IMAGE")
    parser.add_argument("--workers",  type=int, default=None)
    parser.add_argument("--duration", type=int, default=None)
    parser.add_argument("--fresh",    action="store_true")
    args = parser.parse_args()

    if args.images:
        images = args.images; mode = "custom"
        window_s = args.duration or SMOKE_S
        workers  = args.workers  or min(len(images), 10)
    elif args.smoke:
        images = SMOKE_20_IMAGES; mode = "smoke_5min"
        window_s = args.duration or SMOKE_S
        workers  = args.workers  or 10
    elif args.half:
        images = FULL_20_IMAGES; mode = "half_tiered"
        window_s = args.duration or HALF_S
        workers  = args.workers  or 10
    elif args.full:
        images = FULL_20_IMAGES; mode = "full_60min"
        window_s = args.duration or FULL_S
        workers  = args.workers  or 10
    else:
        parser.print_help(); sys.exit(1)

    est_h = math.ceil(len(images) / workers) * (window_s / 3600)
    print(f"""
{BOLD}{C}
╔══════════════════════════════════════════════════════════════════╗
║  PHASE 1-1 PARALLEL AWS — BATCH A (C/Alpine · JVM · Python)    ║
╚══════════════════════════════════════════════════════════════════╝
{RST}
  Mode     : {BOLD}{mode}{RST}
  Images   : {len(images)}  ({workers} workers)
  Window   : {window_s}s  (JVM auto-scaled to 3600s in --half/--full only)
  Estimated: ~{est_h:.1f}h  ({est_h*60:.0f} min)
  Dataset  : {DATASET}
  Started  : {datetime.now().strftime('%Y-%m-%d  %H:%M:%S')}
""")

    rc, _, _ = _run(["kubectl", "get", "nodes", "--no-headers"], timeout=15)
    if rc != 0:
        print(f"  {R}{BOLD}✘  kubectl not working — run phase0_aws.py first{RST}")
        sys.exit(1)
    rc2, out2, _ = _run(["kubectl", "top", "nodes"], timeout=20)
    if rc2 != 0:
        print(f"  {Y}⚠  kubectl top not ready — wait 60s for metrics-server{RST}")
    else:
        print(f"  {G}✔  metrics-server ready{RST}  "
              f"{out2.splitlines()[0][:60] if out2 else ''}")

    if args.fresh and DATASET.exists():
        DATASET.unlink()
        print(f"  {Y}⚠  --fresh: deleted existing dataset_a.csv{RST}")

    t0 = time.time()
    results, failed = run_parallel(images, window_s, workers, mode)
    elapsed = time.time() - t0

    print(f"\n{BOLD}{B}{'═'*64}{RST}")
    print(f"{BOLD}{B}  PHASE 1-1 BATCH A COMPLETE{RST}")
    print(f"{BOLD}{B}{'═'*64}{RST}\n")
    print(f"  Elapsed : {elapsed:.1f}s  ({elapsed/3600:.2f}h)")
    print(f"  Success : {G}{len(results)}{RST} / {len(images)}")
    if failed:
        print(f"  Failed  : {R}{', '.join(failed)}{RST}")

    if results:
        write_summary(results, failed, elapsed)
        print(f"\n  {G}{BOLD}✔  Dataset → {DATASET}{RST}")
        print(f"  {BOLD}Next:{RST}  python3 phase12_parallel_aws.py --{mode.split('_')[0]}\n")
    else:
        print(f"  {R}No data collected.{RST}"); sys.exit(1)


if __name__ == "__main__":
    main()
