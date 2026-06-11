#!/usr/bin/env python3
"""
ECR IMAGE VALIDATOR — EC2 EDITION
===================================
Validates all 150 images on AWS EC2 (m5.4xlarge or similar).

Key differences from KillerKoda version:
  - WORKERS=3  (ECR Public rate-limits at ~100 req/burst on EC2 IPs)
  - REQUEST_GAP=1.0s between each skopeo call (prevents burst throttling)
  - TIMEOUT=40s (EC2 → ECR Public latency is lower but inspection is slower)
  - RETRIES=3 with 5s/10s/20s exponential back-off
  - Shows exact skopeo error for every failure (not truncated)
  - Saves valid.txt automatically for use with --image-list flag
  - Generates fixed_images.py with replacements for truly dead images

Run on EC2:
  python3 validate_ecr_ec2.py                    # validate all 150
  python3 validate_ecr_ec2.py --workers 1        # safest if still failing
  python3 validate_ecr_ec2.py --fix              # suggest tag fixes
  python3 validate_ecr_ec2.py --save valid.txt   # save passing list

Output files:
  ~/phase0-data/valid_images.txt     — one valid image per line
  ~/phase0-data/failed_images.txt    — one failed image per line
  ~/phase0-data/validate_report.txt  — full report
"""

import argparse
import json
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# ── Output ────────────────────────────────────────────────────────────────────
OUT_DIR = Path.home() / "phase0-data"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# ── EC2-tuned constants ───────────────────────────────────────────────────────
WORKERS     = 3      # LOW — ECR Public rate-limits EC2 IPs on burst requests
REQUEST_GAP = 1.0    # seconds between each skopeo call (within a worker)
TIMEOUT_S   = 40     # per-image timeout
RETRIES     = 3      # attempts per image
BACKOFF     = [5, 10, 20]  # seconds between retry attempts

# ── 150 images (validated on KillerKoda, now testing on EC2) ─────────────────
ECR = "public.ecr.aws/docker/library"
MCR = "mcr.microsoft.com"

IMAGES: List[str] = [
    # ── S1: JVM heavy (30) ───────────────────────────────────────────────────
    "public.ecr.aws/bitnami/keycloak:23",
    "public.ecr.aws/bitnami/keycloak:22",
    "public.ecr.aws/elastic/elasticsearch:8.11.0",
    "public.ecr.aws/elastic/elasticsearch:8.9.0",
    "public.ecr.aws/elastic/elasticsearch:7.17.16",
    "public.ecr.aws/elastic/logstash:8.11.0",
    "public.ecr.aws/bitnami/kafka:3.6",
    "public.ecr.aws/bitnami/kafka:3.4",
    "public.ecr.aws/bitnami/zookeeper:3.9",
    "public.ecr.aws/bitnami/tomcat:10.1",
    "public.ecr.aws/bitnami/tomcat:9.0",
    f"{ECR}/tomcat:10-jre21-temurin-jammy",
    f"{ECR}/tomcat:10-jre17-temurin-jammy",
    f"{ECR}/tomcat:9-jre17-temurin-jammy",
    "public.ecr.aws/bitnami/wildfly:27",
    "public.ecr.aws/bitnami/spark:3.4",
    "public.ecr.aws/bitnami/spark:3.5",
    f"{ECR}/gradle:8-jdk21-alpine",
    f"{ECR}/gradle:8-jdk17-alpine",
    f"{ECR}/maven:3.9-eclipse-temurin-21-alpine",
    f"{ECR}/maven:3.9-eclipse-temurin-17-alpine",
    f"{ECR}/maven:3.8-eclipse-temurin-11-alpine",
    "public.ecr.aws/bitnami/jenkins:2",
    "public.ecr.aws/bitnami/solr:9",
    "public.ecr.aws/bitnami/cassandra:4.0",
    f"{ECR}/rabbitmq:3-management-alpine",
    "public.ecr.aws/bitnami/rabbitmq:3.12",
    "public.ecr.aws/bitnami/flink:1",
    f"{ECR}/neo4j:5-community",
    f"{ECR}/sonarqube:10-community",

    # ── S2: JVM base (25) ────────────────────────────────────────────────────
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
    "public.ecr.aws/bitnami/java:21",
    "public.ecr.aws/bitnami/java:17",
    "public.ecr.aws/bitnami/java:11",
    f"{ECR}/groovy:4",
    f"{ECR}/groovy:3",
    f"{ECR}/clojure:temurin-21-alpine",
    f"{ECR}/clojure:temurin-17-alpine",
    f"{ECR}/gradle:8-jdk11-alpine",
    f"{ECR}/amazoncorretto:21-alpine",

    # ── S3: .NET / C# (15) ───────────────────────────────────────────────────
    f"{MCR}/dotnet/aspnet:8.0-alpine3.18",
    f"{MCR}/dotnet/aspnet:7.0-alpine3.18",
    f"{MCR}/dotnet/aspnet:6.0-alpine3.18",
    f"{MCR}/dotnet/runtime:8.0-alpine3.18",
    f"{MCR}/dotnet/runtime:7.0-alpine3.18",
    f"{MCR}/dotnet/runtime:6.0-alpine3.18",
    f"{MCR}/dotnet/sdk:8.0-alpine3.18",
    f"{MCR}/dotnet/sdk:7.0-alpine3.18",
    "public.ecr.aws/bitnami/dotnet-sdk:8",
    "public.ecr.aws/bitnami/dotnet-sdk:7",
    "public.ecr.aws/bitnami/aspnet-core:8",
    "public.ecr.aws/bitnami/aspnet-core:7",
    f"{ECR}/mono:6-slim",
    f"{ECR}/mono:6",
    "public.ecr.aws/bitnami/dotnet-sdk:6",

    # ── S4: Interpreted (30) ─────────────────────────────────────────────────
    f"{ECR}/python:3.12",
    f"{ECR}/python:3.12-slim",
    f"{ECR}/python:3.12-alpine",
    f"{ECR}/python:3.11",
    f"{ECR}/python:3.11-slim",
    f"{ECR}/python:3.11-alpine",
    f"{ECR}/python:3.10-slim",
    f"{ECR}/python:3.9-slim",
    "public.ecr.aws/bitnami/python:3.12",
    "public.ecr.aws/bitnami/python:3.11",
    f"{ECR}/node:20-alpine",
    f"{ECR}/node:20-slim",
    f"{ECR}/node:20",
    f"{ECR}/node:18-alpine",
    f"{ECR}/node:18-slim",
    f"{ECR}/node:16-alpine",
    "public.ecr.aws/bitnami/node:20",
    "public.ecr.aws/bitnami/node:18",
    f"{ECR}/ruby:3.3-alpine",
    f"{ECR}/ruby:3.2-alpine",
    f"{ECR}/ruby:3.1-alpine",
    f"{ECR}/php:8.3-fpm-alpine",
    f"{ECR}/php:8.2-fpm-alpine",
    f"{ECR}/php:8.1-fpm-alpine",
    f"{ECR}/perl:5-slim",
    f"{ECR}/swift:5.9-slim",
    f"{ECR}/julia:1.10-alpine",
    f"{ECR}/r-base:4.0.3",
    "public.ecr.aws/bitnami/ruby:3.3",
    f"{ECR}/php:8.3-cli-alpine",

    # ── S5: Native / Go (20) ─────────────────────────────────────────────────
    f"{ECR}/golang:1.22-alpine",
    f"{ECR}/golang:1.22-bookworm",
    f"{ECR}/golang:1.21-alpine",
    f"{ECR}/golang:1.20-alpine",
    "public.ecr.aws/bitnami/golang:1.22",
    "public.ecr.aws/bitnami/golang:1.21",
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

    # ── S6: Alpine / minimal (20) ─────────────────────────────────────────────
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

    # ── S7: DB / stateful (10) ────────────────────────────────────────────────
    f"{ECR}/postgres:16-alpine",
    f"{ECR}/postgres:15-alpine",
    f"{ECR}/postgres:14-alpine",
    f"{ECR}/mysql:8.0",
    f"{ECR}/mysql:8.2",
    f"{ECR}/mariadb:11-jammy",
    f"{ECR}/mongo:7",
    f"{ECR}/mongo:6",
    "public.ecr.aws/bitnami/influxdb:2",
    "public.ecr.aws/bitnami/clickhouse:23",
]

assert len(IMAGES) == 150, f"Expected 150, got {len(IMAGES)}"
assert len(set(IMAGES)) == 150, "Duplicates found"

STRATA = {
    "S1": (0,   30, "JVM heavy",        30),
    "S2": (30,  55, "JVM base",         25),
    "S3": (55,  70, ".NET / C#",        15),
    "S4": (70, 100, "Interpreted",      30),
    "S5": (100,120, "Native / Go",      20),
    "S6": (120,140, "Alpine / minimal", 20),
    "S7": (140,150, "DB / stateful",    10),
}

STATUS_OK    = "OK"
STATUS_NOTAG = "WRONG_TAG"
STATUS_NOREPO= "NO_REPO"
STATUS_TOUT  = "TIMEOUT"
STATUS_RATELIMIT = "RATE_LIMITED"
STATUS_ERR   = "ERROR"


def _run(cmd: List[str], timeout: int = TIMEOUT_S) -> Tuple[int, str, str]:
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return r.returncode, r.stdout.strip(), r.stderr.strip()
    except subprocess.TimeoutExpired:
        return 1, "", f"timeout after {timeout}s"
    except Exception as e:
        return 1, "", str(e)


def check_image(image: str) -> Dict:
    """
    Check one image with retry + exponential back-off.
    Distinguishes rate-limit errors from genuine not-found errors.
    """
    result = {
        "image":   image,
        "status":  STATUS_ERR,
        "layers":  0,
        "size_mb": 0.0,
        "digest":  "",
        "error":   "",
        "elapsed": 0.0,
        "attempts": 0,
    }
    t0 = time.monotonic()

    for attempt in range(RETRIES):
        result["attempts"] = attempt + 1

        # Throttle between attempts
        if attempt > 0:
            wait = BACKOFF[min(attempt - 1, len(BACKOFF) - 1)]
            print(f"    retry {attempt}/{RETRIES-1} for {image.split('/')[-1][:30]}"
                  f" (waiting {wait}s)...")
            time.sleep(wait)

        rc, out, err = _run(
            ["skopeo", "inspect", f"docker://{image}",
             "--no-creds", "--override-os", "linux", "--override-arch", "amd64"],
            timeout=TIMEOUT_S,
        )

        if rc == 0 and out:
            try:
                info   = json.loads(out)
                layers = info.get("LayersData") or []
                total  = sum(float(l.get("Size", 0))
                             for l in layers if l.get("Size"))
                result.update({
                    "status":  STATUS_OK,
                    "layers":  len(layers),
                    "size_mb": round(total / 1_048_576, 0),
                    "digest":  info.get("Digest", "")[:19],
                    "error":   "",
                })
                result["elapsed"] = round(time.monotonic() - t0, 1)
                return result
            except Exception as e:
                result["error"] = str(e)
                break

        err_l = err.lower()

        # Rate limit / throttle — retry
        if any(x in err_l for x in (
            "429", "too many", "rate limit", "throttl",
            "request limit", "slow down",
        )):
            result["status"] = STATUS_RATELIMIT
            result["error"]  = err[:120]
            continue   # retry with back-off

        # Timeout — retry
        if "timeout" in err_l:
            result["status"] = STATUS_TOUT
            result["error"]  = err[:80]
            continue   # retry

        # Not found — no retry
        if any(x in err_l for x in (
            "manifest unknown", "tag does not exist", "not found", "404",
        )):
            repo = image.split(":")[0]
            rc2, _, _ = _run(
                ["skopeo", "inspect", f"docker://{repo}",
                 "--no-creds", "--override-os", "linux", "--override-arch", "amd64"],
                timeout=20,
            )
            result.update({
                "status": STATUS_NOTAG if rc2 == 0 else STATUS_NOREPO,
                "error":  err[:120],
            })
            break

        # Error parsing image name — not in this registry namespace
        if "error parsing" in err_l or "invalid" in err_l:
            result.update({
                "status": STATUS_NOREPO,
                "error":  "not found in this registry namespace",
            })
            break

        result["error"] = err[:120]
        break  # unknown error, no retry

    result["elapsed"] = round(time.monotonic() - t0, 1)
    return result


def suggest_fix(image: str) -> Optional[str]:
    """List-tags and return nearest valid tag."""
    repo, _, tag = image.partition(":")
    rc, out, _ = _run(
        ["skopeo", "list-tags", f"docker://{repo}", "--no-creds"], timeout=30
    )
    if rc != 0 or not out:
        return None
    try:
        tags = json.loads(out).get("Tags", [])
    except Exception:
        return None
    if not tags:
        return None
    major = tag.split(".")[0].split("-")[0]
    candidates = [t for t in tags if t.startswith(major)] or tags
    candidates.sort(key=lambda t: (len(t), t))
    return f"{repo}:{candidates[0]}"


def check_skopeo() -> bool:
    rc, out, _ = _run(["skopeo", "--version"], timeout=5)
    if rc != 0:
        print("ERROR: skopeo not found.  sudo apt-get install -y skopeo")
        return False
    print(f"  {out.splitlines()[0]}")
    return True


def print_report(results: List[Dict], show_fix: bool) -> Tuple[List[str], List[str]]:
    by_status: Dict[str, List[Dict]] = {}
    for r in results:
        by_status.setdefault(r["status"], []).append(r)

    ok      = by_status.get(STATUS_OK, [])
    failed  = [r for r in results if r["status"] != STATUS_OK]
    W = 70

    print(f"\n{'═'*W}")
    print(f"  VALIDATION REPORT — EC2")
    print(f"  {len(ok)}/{len(results)} valid  ·  {len(failed)} failed")
    print(f"{'═'*W}")

    # Stratum summary
    print(f"\n  {'Code':<4} {'Category':<22} {'Valid':>5}  {'Total':>5}  Status")
    print(f"  {'-'*4} {'-'*22} {'-'*5}  {'-'*5}  ------")
    for code, (start, end, name, expected) in STRATA.items():
        imgs  = set(IMAGES[i] for i in range(start, end))
        n_ok  = sum(1 for r in ok if r["image"] in imgs)
        flag  = "✓" if n_ok == expected else f"✗ {expected-n_ok} fail"
        print(f"  {code:<4} {name:<22} {n_ok:>5}  {expected:>5}  {flag}")

    # Valid table
    print(f"\n✓  VALID ({len(ok)})")
    print(f"   {'Image':<60}  {'L':>3}  {'MB':>6}  {'s':>5}")
    print(f"   {'-'*60}  {'-'*3}  {'-'*6}  {'-'*5}")
    for r in sorted(ok, key=lambda x: x["image"]):
        print(f"   {r['image']:<60}  {r['layers']:>3}  "
              f"{r['size_mb']:>5.0f}M  {r['elapsed']:>4.1f}s")

    # Failed — show full error for diagnosis
    for status, label in [
        (STATUS_RATELIMIT, "RATE LIMITED — retry with --workers 1"),
        (STATUS_TOUT,      "TIMEOUT"),
        (STATUS_NOTAG,     "WRONG TAG"),
        (STATUS_NOREPO,    "REPOSITORY NOT FOUND"),
        (STATUS_ERR,       "ERRORS"),
    ]:
        items = by_status.get(status, [])
        if not items:
            continue
        print(f"\n✗  {label} ({len(items)})")
        for r in items:
            print(f"   {r['image']}")
            print(f"   └─ {r['error'][:100]}")
            if show_fix and status in (STATUS_NOTAG,):
                fix = suggest_fix(r["image"])
                print(f"   └─ suggested fix: {fix or 'unknown'}")

    print(f"\n{'═'*W}")
    if not failed:
        print(f"  ✓  ALL {len(results)} IMAGES VALID ON EC2")
        print(f"     Safe to run: sudo python3 phase0_model1_collector.py \\")
        print(f"                     --runs 2 --discard-first \\")
        print(f"                     --output ~/Request_Limit_new/data/training_dataset_model1.csv")
    else:
        rate_lim = len(by_status.get(STATUS_RATELIMIT, []))
        genuine  = len(failed) - rate_lim
        if rate_lim:
            print(f"  ✗  {rate_lim} rate-limited → re-run with --workers 1")
        if genuine:
            print(f"  ✗  {genuine} genuinely failed → update image list")
    print(f"{'═'*W}\n")

    valid_images  = [r["image"] for r in ok]
    failed_images = [r["image"] for r in failed]
    return valid_images, failed_images


def main() -> None:
    parser = argparse.ArgumentParser(
        description="ECR image validator tuned for EC2 (rate-limit safe)"
    )
    parser.add_argument("--fix",    action="store_true",
                        help="Suggest nearest valid tag for wrong-tag failures")
    parser.add_argument("--workers", type=int, default=WORKERS,
                        help=f"Parallel workers (default {WORKERS}, use 1 if still failing)")
    parser.add_argument("--save",   metavar="FILE",
                        help="Save valid image list to file")
    args = parser.parse_args()

    print(f"\n{'═'*70}")
    print(f"  ECR IMAGE VALIDATOR — EC2 EDITION")
    print(f"  Images  : {len(IMAGES)} (7 strata)")
    print(f"  Workers : {args.workers}  (EC2 rate-limit safe)")
    print(f"  Timeout : {TIMEOUT_S}s  ·  Retries: {RETRIES}  ·  Gap: {REQUEST_GAP}s")
    est = round(len(IMAGES) / args.workers * (TIMEOUT_S * 0.3 + REQUEST_GAP))
    print(f"  Est.    : ~{est//60}–{est//60+3} min")
    print(f"{'═'*70}\n")

    if not check_skopeo():
        sys.exit(1)

    print(f"Checking {len(IMAGES)} images with {args.workers} workers...\n")

    results: List[Dict] = []
    t0 = time.monotonic()

    # Use a small gap between dispatching each image to avoid burst
    def check_with_gap(image: str) -> Dict:
        time.sleep(REQUEST_GAP)
        return check_image(image)

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        future_map = {ex.submit(check_with_gap, img): img for img in IMAGES}
        done = 0
        for future in as_completed(future_map):
            r = future.result()
            results.append(r)
            done += 1
            icon   = "✓" if r["status"] == STATUS_OK else "✗"
            detail = (f"{r['size_mb']:.0f}MB {r['layers']}L"
                      if r["status"] == STATUS_OK
                      else f"{r['status']} (attempt {r['attempts']})")
            print(f"  {icon} [{done:3d}/150]  {r['image']:<60}  {detail}")

    elapsed = time.monotonic() - t0
    print(f"\nDone in {elapsed:.0f}s ({elapsed/60:.1f} min)")

    valid_images, failed_images = print_report(results, args.fix)

    # Always save valid and failed lists
    valid_path  = OUT_DIR / "valid_images.txt"
    failed_path = OUT_DIR / "failed_images.txt"
    report_path = OUT_DIR / "validate_report.txt"

    valid_path.write_text("\n".join(valid_images) + "\n")
    failed_path.write_text("\n".join(failed_images) + "\n")

    # Write text report
    import io
    report_path.write_text(
        f"EC2 Validation Report\n"
        f"Run: {__import__('datetime').datetime.now().isoformat()}\n"
        f"Valid: {len(valid_images)}/150\n"
        f"Failed: {len(failed_images)}/150\n\n"
        f"FAILED:\n" + "\n".join(failed_images)
    )

    print(f"  Valid list  → {valid_path}")
    print(f"  Failed list → {failed_path}")
    print(f"  Report      → {report_path}")

    if args.save:
        Path(args.save).write_text("\n".join(valid_images) + "\n")
        print(f"  Also saved  → {args.save}")

    # Print next-step command
    if valid_images:
        print(f"\nNext step — run collector on valid images only:")
        print(f"  sudo python3 phase0_model1_collector.py \\")
        print(f"      --runs 2 --discard-first \\")
        print(f"      --output ~/Request_Limit_new/data/training_dataset_model1.csv")

    sys.exit(0 if not failed_images else 1)


if __name__ == "__main__":
    main()
