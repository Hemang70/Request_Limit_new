#!/usr/bin/env bash
# =============================================================================
# bootstrap_aws.sh  —  Full environment setup for m5.4xlarge (Ubuntu 22.04/24)
# Zero-Telemetry CPU Prediction  /  Carbon-Aware Right-Sizing
#
# Run once on a fresh EC2 instance.  Safe to re-run (all steps are idempotent).
# Expected total time: ~13 minutes
#
# Fixes vs previous version:
#   • Waits for unattended-upgrades apt lock before any apt call
#   • k3s installed with --write-kubeconfig-mode 644 (no sudo cp needed)
#   • numpy pin removed — uses wheel-first install for Python 3.12/3.14
#   • python3-dev + gcc added for source builds when no wheel available
#
# Usage:
#   chmod +x bootstrap_aws.sh && ./bootstrap_aws.sh
#   ./bootstrap_aws.sh 2>&1 | tee bootstrap.log
# =============================================================================
set -euo pipefail

G="\033[92m"; R="\033[91m"; Y="\033[93m"; B="\033[94m"
RST="\033[0m"; BOLD="\033[1m"; DIM="\033[2m"

ok()  { echo -e "  ${G}✔${RST}  $*"; }
fail(){ echo -e "  ${R}✘${RST}  $*"; exit 1; }
info(){ echo -e "  ${Y}→${RST}  $*"; }
hdr() { echo -e "\n${BOLD}${B}── $* ──${RST}"; }

# =============================================================================
# STEP 0 — Instance preflight
# =============================================================================
hdr "Step 0 — Instance preflight"
VCPUS=$(nproc)
RAM_GB=$(free -g | awk '/^Mem/{print $2}')
info "vCPUs  : $VCPUS"
info "RAM    : ${RAM_GB} GB"
[ "$VCPUS" -ge 8  ] || echo -e "  ${Y}⚠  Only $VCPUS vCPU(s) — recommended ≥16 (m5.4xlarge)${RST}"
[ "$RAM_GB" -ge 16 ] || echo -e "  ${Y}⚠  Only ${RAM_GB} GB RAM — recommended ≥32 GB${RST}"
DISK_GB=$(df -BG / | awk 'NR==2{gsub("G","",$4); print $4}')
info "Disk   : ${DISK_GB} GB free"
[ "$DISK_GB" -ge 20 ] || fail "Less than 20 GB free — resize EBS first"
ok "Preflight passed"

# =============================================================================
# STEP 1 — Wait for apt lock, then install base packages
# =============================================================================
hdr "Step 1 — System packages"

# AWS Ubuntu runs unattended-upgrades on first boot and holds the dpkg lock
# for several minutes.  Wait up to 5 min before touching apt.
info "Waiting for apt lock to be free (up to 5 min on fresh instance)..."
WAITED=0
while sudo fuser /var/lib/dpkg/lock-frontend >/dev/null 2>&1; do
    echo -e "  ${DIM}  apt locked — waiting 10s (${WAITED}s elapsed)...${RST}"
    sleep 10
    WAITED=$((WAITED + 10))
    [ "$WAITED" -ge 300 ] && {
        info "Lock held too long — killing unattended-upgrades and continuing"
        sudo killall unattended-upgrades 2>/dev/null || true
        sudo rm -f /var/lib/dpkg/lock-frontend /var/lib/dpkg/lock \
                   /var/cache/apt/archives/lock
        sudo dpkg --configure -a 2>/dev/null || true
        break
    }
done
ok "apt lock free"

sudo apt-get update -qq

info "Installing base tools + build dependencies..."
sudo apt-get install -y -q --no-install-recommends \
    curl wget git jq unzip \
    ca-certificates gnupg lsb-release \
    apt-transport-https software-properties-common \
    python3 python3-venv python3-pip \
    python3-dev gcc gfortran pkg-config \
    docker.io \
    socat conntrack ipset
ok "Base packages installed"

sudo systemctl enable docker --now 2>/dev/null || true
sudo usermod -aG docker "$USER" 2>/dev/null || true
ok "Docker enabled"

# =============================================================================
# STEP 2 — k3s  (--write-kubeconfig-mode 644 avoids permission errors)
# =============================================================================
hdr "Step 2 — k3s Kubernetes"

if command -v k3s &>/dev/null && k3s kubectl get nodes &>/dev/null 2>&1; then
    ok "k3s already installed and running"
else
    info "Installing k3s..."
    curl -sfL https://get.k3s.io | \
        INSTALL_K3S_EXEC="--disable traefik --disable servicelb --write-kubeconfig-mode 644" sh -
    ok "k3s installed"
fi

# kubeconfig — works whether k3s just installed or was already present
info "Configuring kubeconfig..."
mkdir -p "$HOME/.kube"
sudo cp /etc/rancher/k3s/k3s.yaml "$HOME/.kube/config"
sudo chown "$USER:$USER" "$HOME/.kube/config"
chmod 600 "$HOME/.kube/config"

info "Waiting for k3s node Ready (up to 90s)..."
for i in $(seq 1 18); do
    STATUS=$(kubectl get nodes --no-headers 2>/dev/null | awk '{print $2}' | head -1)
    if [ "$STATUS" = "Ready" ]; then
        ok "k3s node Ready"; break
    fi
    echo -e "  ${DIM}  [$i/18] status: ${STATUS:-Pending}  (waiting 5s)${RST}"
    sleep 5
done
kubectl get nodes --no-headers 2>/dev/null | grep -q Ready || \
    echo -e "  ${Y}⚠  Node not yet Ready — continue anyway${RST}"

# =============================================================================
# STEP 3 — kubectl  (k3s ships its own; this installs the standalone binary)
# =============================================================================
hdr "Step 3 — kubectl"

if command -v kubectl &>/dev/null; then
    VER=$(kubectl version --client 2>/dev/null | head -1)
    ok "kubectl already installed: $VER"
else
    info "Installing kubectl..."
    curl -sfLO "https://dl.k8s.io/release/$(curl -sL https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
    chmod +x kubectl && sudo mv kubectl /usr/local/bin/
    ok "kubectl installed"
fi

# =============================================================================
# STEP 4 — helm
# =============================================================================
hdr "Step 4 — Helm"

if command -v helm &>/dev/null; then
    ok "helm already installed: $(helm version --short 2>/dev/null)"
else
    info "Installing helm..."
    curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
    ok "helm installed"
fi

# =============================================================================
# STEP 5 — metrics-server  (required for kubectl top)
# =============================================================================
hdr "Step 5 — metrics-server"

if kubectl get deployment metrics-server -n kube-system --no-headers &>/dev/null 2>&1; then
    ok "metrics-server already deployed"
else
    helm repo add metrics-server https://kubernetes-sigs.github.io/metrics-server/ --force-update
    helm repo update
    helm upgrade --install metrics-server metrics-server/metrics-server \
        --namespace kube-system \
        --set args="{--kubelet-insecure-tls}" \
        --wait --timeout 3m
    ok "metrics-server installed"
fi

# =============================================================================
# STEP 6 — skopeo
# =============================================================================
hdr "Step 6 — skopeo"

if command -v skopeo &>/dev/null; then
    ok "skopeo already installed: $(skopeo --version)"
else
    sudo apt-get install -y -q skopeo
    ok "skopeo installed: $(skopeo --version)"
fi

# =============================================================================
# STEP 7 — k6  (Grafana repo)
# =============================================================================
hdr "Step 7 — k6"

if command -v k6 &>/dev/null; then
    ok "k6 already installed: $(k6 version | head -1)"
else
    info "Installing k6..."
    sudo gpg --no-default-keyring \
        --keyring /usr/share/keyrings/k6-archive-keyring.gpg \
        --keyserver hkp://keyserver.ubuntu.com:80 \
        --recv-keys C5AD17C747E3415A3642D57D77C6C491D6AC1D69 2>/dev/null
    echo "deb [signed-by=/usr/share/keyrings/k6-archive-keyring.gpg] \
https://dl.k6.io/deb stable main" \
        | sudo tee /etc/apt/sources.list.d/k6.list > /dev/null
    sudo apt-get update -qq
    sudo apt-get install -y -q k6
    ok "k6 installed: $(k6 version | head -1)"
fi

# =============================================================================
# STEP 8 — Python venv
# Numpy strategy:
#   1. Try --only-binary=:all: first (fast, no compiler needed, works on 3.12+)
#   2. Fall back to source build only if no wheel exists (needs gcc/gfortran)
# numpy<2 pin REMOVED — numpy 2.x is required for Python 3.13/3.14
# =============================================================================
hdr "Step 8 — Python venv (~/.venv-zerotelem)"

VENV="$HOME/.venv-zerotelem"
VPIP="$VENV/bin/pip"
VPY="$VENV/bin/python"

if [ -f "$VPY" ]; then
    MISSING=0
    for pkg in pandas sklearn xgboost shap matplotlib codecarbon; do
        "$VPY" -c "import $pkg" 2>/dev/null || { MISSING=1; break; }
    done
    if [ "$MISSING" -eq 0 ]; then
        ok "All Python packages already in venv — skipping"
    else
        info "Venv incomplete — rebuilding..."
        rm -rf "$VENV"
    fi
fi

if [ ! -f "$VPY" ]; then
    info "Creating venv..."
    python3 -m venv "$VENV"
    "$VPIP" install --quiet --upgrade pip setuptools wheel

    info "Installing numpy (wheel-first, no compiler if wheel available)..."
    if "$VPIP" install --quiet --only-binary=:all: numpy; then
        ok "numpy installed from wheel"
    else
        info "No wheel for this Python version — compiling numpy (needs gfortran, ~5 min)..."
        "$VPIP" install --quiet numpy
    fi

    info "Installing ML packages..."
    "$VPIP" install --quiet --no-warn-script-location \
        pandas scikit-learn xgboost shap \
        matplotlib seaborn tqdm tabulate \
        codecarbon requests pyyaml
    ok "Python packages installed"
fi

# .pth bridge
SP=$(ls -d "$VENV"/lib/python*/site-packages 2>/dev/null | head -1)
if [ -n "$SP" ]; then
    for SD in $(python3 -c "import site; print('\n'.join(site.getsitepackages()))" 2>/dev/null); do
        echo "$SP" | sudo tee "$SD/zerotelem_venv.pth" > /dev/null 2>&1 && break || true
    done
fi
ok "venv bridge written"

# =============================================================================
# STEP 9 — Project directories
# =============================================================================
hdr "Step 9 — Project directories"

for DIR in \
    "$HOME/zero-telemetry-cpu" \
    "$HOME/zero-telemetry-cpu/data" \
    "$HOME/zero-telemetry-cpu/data/cpu_traces" \
    "$HOME/zero-telemetry-cpu/results" \
    "$HOME/zero-telemetry-cpu/models" \
    "$HOME/zero-telemetry-cpu/logs"
do
    mkdir -p "$DIR"
done
ok "Project tree: ~/zero-telemetry-cpu/"

# =============================================================================
# STEP 10 — Docker Hub auth reminder
# =============================================================================
hdr "Step 10 — Docker Hub"

if [ -f "$HOME/.docker/config.json" ] && \
   grep -q "index.docker.io" "$HOME/.docker/config.json" 2>/dev/null; then
    ok "Docker Hub credentials already configured"
else
    echo -e "  ${Y}⚠  Anonymous pulls limited to 100/6h.  Run: docker login${RST}"
fi

# =============================================================================
# STEP 11 — Functional smoke tests
# =============================================================================
hdr "Step 11 — Functional smoke tests"

kubectl get nodes --no-headers > /dev/null 2>&1 && \
    ok "kubectl → cluster reachable" || \
    echo -e "  ${Y}⚠  kubectl not reaching cluster${RST}"

info "Testing kubectl top (up to 60s for metrics-server)..."
for i in 1 2 3; do
    kubectl top nodes > /dev/null 2>&1 && { ok "kubectl top → working"; break; }
    [ "$i" -lt 3 ] && { echo -e "  ${DIM}  attempt $i/3, waiting 20s...${RST}"; sleep 20; }
done
kubectl top nodes > /dev/null 2>&1 || \
    echo -e "  ${Y}⚠  kubectl top not ready — phase0_aws.py will retry${RST}"

skopeo inspect --raw docker://docker.io/library/nginx:alpine > /dev/null 2>&1 && \
    ok "skopeo → nginx:alpine manifest pulled" || \
    echo -e "  ${Y}⚠  skopeo could not reach Docker Hub${RST}"

TMP=$(mktemp /tmp/k6test.XXXXXX.js)
cat > "$TMP" <<'EOF'
import http from 'k6/http';
import { sleep } from 'k6';
export const options = { vus: 1, duration: '3s' };
export default function () { http.get('http://test.k6.io'); sleep(1); }
EOF
k6 run --quiet "$TMP" > /dev/null 2>&1 && ok "k6 → 3s test passed" || \
    echo -e "  ${Y}⚠  k6 test non-zero (network issue?)${RST}"
rm -f "$TMP"

"$VPY" -c "
import pandas, sklearn, xgboost, shap, matplotlib, codecarbon
print('  \033[92m✔\033[0m  Python packages → all importable')
" 2>/dev/null || echo -e "  ${Y}⚠  Some Python packages failed to import${RST}"

docker manifest inspect nginx:alpine > /dev/null 2>&1 && \
    ok "docker manifest inspect → working" || \
    echo -e "  ${Y}⚠  docker manifest failed — run: newgrp docker${RST}"

# =============================================================================
# DONE
# =============================================================================
echo -e "
${BOLD}${B}══════════════════════════════════════════════════════════════${RST}
${BOLD}${B}  BOOTSTRAP COMPLETE${RST}
${BOLD}${B}══════════════════════════════════════════════════════════════${RST}

  Installed:
    k3s          $(k3s --version 2>/dev/null | head -1)
    kubectl      $(kubectl version --client 2>/dev/null | head -1)
    helm         $(helm version --short 2>/dev/null)
    skopeo       $(skopeo --version 2>/dev/null)
    k6           $(k6 version 2>/dev/null | head -1)
    Python venv  $VENV  [$(\"$VPY\" --version 2>/dev/null)]
    numpy        $( \"$VPY\" -c 'import numpy; print(numpy.__version__)' 2>/dev/null)
    metrics-srv  $(kubectl get deploy metrics-server -n kube-system --no-headers 2>/dev/null | awk '{print \$2}' || echo 'check kubectl')

  Next steps:
    ./benchmark_install.sh                         # install redis-bm, sysbench, pgbench …
    python3 phase0_aws.py                          # 4-step verification
    python3 validate_setup.py                      # 15-check pre-flight
    python3 phase1_parallel_aws.py --smoke         # 5-min smoke test
    python3 phase1_parallel_aws.py --half          # full dataset (~35 min)

  If docker manifest errors: newgrp docker  OR  log out and back in
"

