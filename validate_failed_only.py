#!/usr/bin/env python3
"""
VALIDATE FAILED IMAGES ONLY — EC2
===================================
Checks only the 32 replacement images from Validate_Ecr_150_images.py
(the ones that replaced failed bitnami images).
Takes ~3–5 minutes instead of 15+.

Run:
  python3 validate_failed_only.py
  python3 validate_failed_only.py --workers 1   # if still rate-limited
"""

import json, subprocess, sys, time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Tuple

ECR = "public.ecr.aws/docker/library"
MCR = "mcr.microsoft.com"

# ── Only the 32 replacement images ───────────────────────────────────────────
REPLACEMENTS: List[Tuple[str, str]] = [
    # (old_bitnami_image,  new_replacement_image)
    # S1 JVM heavy (15)
    ("bitnami/keycloak:23",     f"{ECR}/tomcat:9-jre11-temurin-jammy"),
    ("bitnami/keycloak:22",     f"{ECR}/tomcat:10-jre11-temurin-jammy"),
    ("bitnami/kafka:3.6",       f"{ECR}/eclipse-temurin:21-jdk-jammy"),
    ("bitnami/kafka:3.4",       f"{ECR}/eclipse-temurin:17-jdk-jammy"),
    ("bitnami/zookeeper:3.9",   f"{ECR}/eclipse-temurin:11-jdk-jammy"),
    ("bitnami/tomcat:10.1",     f"{ECR}/eclipse-temurin:8-jdk-jammy"),
    ("bitnami/tomcat:9.0",      f"{ECR}/gradle:8-jdk11-bookworm"),
    ("bitnami/wildfly:27",      f"{ECR}/maven:3.8-eclipse-temurin-8-alpine"),
    ("bitnami/spark:3.4",       f"{ECR}/maven:3.6-eclipse-temurin-8-alpine"),
    ("bitnami/spark:3.5",       f"{ECR}/flink:scala_2.12-java17"),
    ("bitnami/jenkins:2",       f"{ECR}/flink:scala_2.12-java11"),
    ("bitnami/solr:9",          f"{ECR}/jetty:12-jre21-alpine"),
    ("bitnami/cassandra:4.0",   f"{ECR}/cassandra:5"),
    ("bitnami/rabbitmq:3.12",   f"{ECR}/cassandra:4"),
    ("bitnami/flink:1",         f"{ECR}/jetty:12-jre17-alpine"),
    # S2 JVM base (3)
    ("bitnami/java:21",         f"{ECR}/eclipse-temurin:21-jdk-focal"),
    ("bitnami/java:17",         f"{ECR}/eclipse-temurin:17-jdk-focal"),
    ("bitnami/java:11",         f"{ECR}/eclipse-temurin:11-jdk-focal"),
    # S3 .NET (5)
    ("bitnami/dotnet-sdk:8",    f"{MCR}/dotnet/sdk:8.0"),
    ("bitnami/dotnet-sdk:7",    f"{MCR}/dotnet/sdk:7.0"),
    ("bitnami/aspnet-core:8",   f"{MCR}/dotnet/aspnet:8.0"),
    ("bitnami/aspnet-core:7",   f"{MCR}/dotnet/aspnet:7.0"),
    ("bitnami/dotnet-sdk:6",    f"{MCR}/dotnet/sdk:6.0"),
    # S4 Interpreted (5)
    ("bitnami/python:3.12",     f"{ECR}/python:3.12-bookworm"),
    ("bitnami/python:3.11",     f"{ECR}/python:3.11-bookworm"),
    ("bitnami/node:20",         f"{ECR}/node:20-bookworm"),
    ("bitnami/node:18",         f"{ECR}/node:18-bookworm"),
    ("bitnami/ruby:3.3",        f"{ECR}/ruby:3.3-slim"),
    # S5 Native/Go (2)
    ("bitnami/golang:1.22",     f"{ECR}/golang:1.22-bullseye"),
    ("bitnami/golang:1.21",     f"{ECR}/golang:1.21-bullseye"),
    # S7 DB (2)
    ("bitnami/influxdb:2",      f"{ECR}/postgres:13-alpine"),
    ("bitnami/clickhouse:23",   f"{ECR}/mariadb:10-jammy"),
]

NEW_IMAGES = [new for _, new in REPLACEMENTS]
assert len(set(NEW_IMAGES)) == len(NEW_IMAGES), "Duplicates in replacement list"

WORKERS    = 3
TIMEOUT_S  = 40
RETRIES    = 3
BACKOFF    = [5, 10, 20]

def _run(cmd, timeout=TIMEOUT_S):
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return r.returncode, r.stdout.strip(), r.stderr.strip()
    except subprocess.TimeoutExpired:
        return 1, "", f"timeout {timeout}s"
    except Exception as e:
        return 1, "", str(e)

def check(image: str) -> Dict:
    result = {"image": image, "status": "ERR",
              "layers": 0, "size_mb": 0.0, "error": "", "attempts": 0}
    for attempt in range(RETRIES):
        result["attempts"] = attempt + 1
        if attempt > 0:
            wait = BACKOFF[min(attempt-1, len(BACKOFF)-1)]
            print(f"    retry {attempt} for {image.split('/')[-1][:35]} (wait {wait}s)...")
            time.sleep(wait)

        rc, out, err = _run(
            ["skopeo", "inspect", f"docker://{image}",
             "--no-creds", "--override-os", "linux", "--override-arch", "amd64"]
        )
        if rc == 0 and out:
            try:
                info   = json.loads(out)
                layers = info.get("LayersData") or []
                total  = sum(float(l.get("Size",0)) for l in layers if l.get("Size"))
                result.update({
                    "status":  "OK",
                    "layers":  len(layers),
                    "size_mb": round(total/1_048_576, 0),
                    "error":   "",
                })
                return result
            except Exception as e:
                result["error"] = str(e); break

        err_l = err.lower()
        if any(x in err_l for x in ("429","too many","rate limit","throttl")):
            result["status"] = "RATE_LIMITED"; result["error"] = err[:80]
            continue
        if "timeout" in err_l:
            result["status"] = "TIMEOUT"; result["error"] = err[:80]
            continue
        if any(x in err_l for x in ("manifest unknown","not found","404")):
            result["status"] = "NOT_FOUND";  result["error"] = err[:120]; break
        result["error"] = err[:120]; break

    return result

def main():
    n = len(REPLACEMENTS)
    print(f"\n{'═'*68}")
    print(f"  VALIDATE REPLACEMENT IMAGES ONLY ({n} images)")
    print(f"  Checking only the 32 images that replaced failed bitnami refs")
    print(f"  Workers: {WORKERS}  ·  Timeout: {TIMEOUT_S}s  ·  Retries: {RETRIES}")
    print(f"  Est. ~{round(n/WORKERS*12/60)} min")
    print(f"{'═'*68}\n")

    rc, out, _ = _run(["skopeo", "--version"], timeout=5)
    if rc != 0:
        print("ERROR: skopeo not found"); sys.exit(1)
    print(f"  {out.splitlines()[0]}\n")

    results: List[Dict] = []
    t0 = time.monotonic()

    def check_with_gap(pair):
        time.sleep(1.0)
        old, new = pair
        r = check(new)
        r["old"] = old
        return r

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = {ex.submit(check_with_gap, pair): pair for pair in REPLACEMENTS}
        done = 0
        for future in as_completed(futs):
            r = future.result()
            done += 1
            icon   = "✓" if r["status"] == "OK" else "✗"
            detail = (f"{r['size_mb']:.0f}MB {r['layers']}L"
                      if r["status"] == "OK" else r["status"])
            print(f"  {icon} [{done:2d}/{n}]  {r['image']:<58}  {detail}")
            results.append(r)

    elapsed = time.monotonic() - t0
    ok     = [r for r in results if r["status"] == "OK"]
    failed = [r for r in results if r["status"] != "OK"]

    print(f"\nDone in {elapsed:.0f}s")
    print(f"\n{'═'*68}")
    print(f"  RESULT: {len(ok)}/{n} replacements valid")
    print(f"{'═'*68}")

    if ok:
        print(f"\n✓  VALID ({len(ok)})")
        for r in sorted(ok, key=lambda x: x["image"]):
            old_short = r["old"].replace("bitnami/","")
            print(f"   {r['image']:<58}  {r['size_mb']:.0f}MB  "
                  f"(replaced {old_short})")

    if failed:
        print(f"\n✗  STILL FAILING ({len(failed)}) — need further replacement")
        for r in failed:
            old_short = r["old"].replace("bitnami/","")
            print(f"   {r['image']}")
            print(f"   └─ status: {r['status']}  error: {r['error'][:80]}")
            print(f"   └─ was replacing: {old_short}")

    print(f"\n{'═'*68}")
    if not failed:
        print(f"  ✓ ALL {n} REPLACEMENTS VALID")
        print(f"  Update phase0_model1_collector.py with Validate_Ecr_150_images.py")
        print(f"  Then run:")
        print(f"    sudo python3 phase0_model1_collector.py \\")
        print(f"        --runs 2 --discard-first \\")
        print(f"        --output ~/Request_Limit_new/data/training_dataset_model1.csv")
    else:
        print(f"  ✗ {len(failed)} replacements still failing — post output to fix")
    print(f"{'═'*68}\n")

    sys.exit(0 if not failed else 1)

if __name__ == "__main__":
    main()
