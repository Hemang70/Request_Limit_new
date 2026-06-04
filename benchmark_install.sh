#!/usr/bin/env bash
# =============================================================================
# benchmark_install.sh  —  Install all benchmark tools for 40-image dataset
# Run once on the m5.4xlarge instance after bootstrap_aws.sh
# Expected time: ~5 minutes
# =============================================================================
set -euo pipefail
G="\033[92m"; Y="\033[93m"; RST="\033[0m"; BOLD="\033[1m"
ok()  { echo -e "  ${G}✔${RST}  $*"; }
info(){ echo -e "  ${Y}→${RST}  $*"; }

echo -e "\n${BOLD}Installing benchmark tools for 40-image dataset...${RST}\n"

sudo apt-get update -qq

# ── 1. redis-benchmark (ships with redis-tools) ───────────────────────────────
if command -v redis-benchmark &>/dev/null; then
    ok "redis-benchmark already installed"
else
    info "Installing redis-tools..."
    sudo apt-get install -y -q redis-tools
    ok "redis-benchmark: $(redis-benchmark --version 2>/dev/null | head -1)"
fi

# ── 2. memtier-benchmark ──────────────────────────────────────────────────────
if command -v memtier_benchmark &>/dev/null; then
    ok "memtier-benchmark already installed"
else
    info "Installing memtier-benchmark..."
    sudo apt-get install -y -q memtier-benchmark 2>/dev/null || {
        sudo apt-get install -y -q \
            build-essential autoconf automake libpcre3-dev \
            libevent-dev pkg-config zlib1g-dev libssl-dev
        git clone --depth 1 https://github.com/RedisLabs/memtier_benchmark /tmp/memtier
        cd /tmp/memtier && autoreconf -ivf && ./configure && make -j4
        sudo make install
        cd ~
    }
    ok "memtier_benchmark installed"
fi

# ── 3. wrk ────────────────────────────────────────────────────────────────────
if command -v wrk &>/dev/null; then
    ok "wrk already installed"
else
    info "Installing wrk..."
    sudo apt-get install -y -q wrk 2>/dev/null || {
        sudo apt-get install -y -q build-essential libssl-dev
        git clone --depth 1 https://github.com/wg/wrk /tmp/wrk
        make -C /tmp/wrk -j4
        sudo cp /tmp/wrk/wrk /usr/local/bin/
    }
    ok "wrk: $(wrk --version 2>&1 | head -1)"
fi

# ── 4. sysbench ───────────────────────────────────────────────────────────────
if command -v sysbench &>/dev/null; then
    ok "sysbench already installed"
else
    info "Installing sysbench..."
    sudo apt-get install -y -q sysbench
    ok "sysbench: $(sysbench --version)"
fi

# ── 5. pgbench (ships with postgresql-client) ────────────────────────────────
if command -v pgbench &>/dev/null; then
    ok "pgbench already installed"
else
    info "Installing pgbench..."
    sudo apt-get install -y -q postgresql-client
    ok "pgbench: $(pgbench --version)"
fi

# ── 6. mongosh ────────────────────────────────────────────────────────────────
if command -v mongosh &>/dev/null; then
    ok "mongosh already installed"
else
    info "Installing mongosh..."
    wget -qO - https://www.mongodb.org/static/pgp/server-7.0.asc \
        | sudo gpg --dearmor -o /usr/share/keyrings/mongodb-server-7.0.gpg
    echo "deb [ arch=amd64,arm64 signed-by=/usr/share/keyrings/mongodb-server-7.0.gpg ] \
https://repo.mongodb.org/apt/ubuntu jammy/mongodb-org/7.0 multiverse" \
        | sudo tee /etc/apt/sources.list.d/mongodb-org-7.0.list
    sudo apt-get update -qq
    sudo apt-get install -y -q mongodb-mongosh
    ok "mongosh: $(mongosh --version)"
fi

# ── 7. cassandra-stress (ships with cassandra package) ───────────────────────
if command -v cassandra-stress &>/dev/null; then
    ok "cassandra-stress already installed"
else
    info "Installing cassandra-stress..."
    sudo apt-get install -y -q default-jre-headless
    wget -q https://downloads.apache.org/cassandra/4.1.7/apache-cassandra-4.1.7-bin.tar.gz \
        -O /tmp/cassandra.tar.gz
    sudo tar -xzf /tmp/cassandra.tar.gz -C /opt/
    sudo ln -sf /opt/apache-cassandra-4.1.7/tools/bin/cassandra-stress \
        /usr/local/bin/cassandra-stress
    rm /tmp/cassandra.tar.gz
    ok "cassandra-stress installed"
fi

# ── 8. rabbitmq-perf-test ─────────────────────────────────────────────────────
if command -v rabbitmq-perf-test &>/dev/null; then
    ok "rabbitmq-perf-test already installed"
else
    info "Installing rabbitmq-perf-test..."
    sudo apt-get install -y -q default-jre-headless
    PV="2.21.0"
    wget -q "https://github.com/rabbitmq/rabbitmq-perf-test/releases/download/v${PV}/rabbitmq-perf-test-${PV}-bin.tar.gz" \
        -O /tmp/rabbitperf.tar.gz
    sudo tar -xzf /tmp/rabbitperf.tar.gz -C /opt/
    sudo ln -sf "/opt/rabbitmq-perf-test-${PV}/bin/runjava" \
        /usr/local/bin/rabbitmq-perf-test
    rm /tmp/rabbitperf.tar.gz
    ok "rabbitmq-perf-test ${PV} installed"
fi

# ── 9. kafka-producer-perf-test (ships with kafka binaries) ──────────────────
if command -v kafka-producer-perf-test.sh &>/dev/null; then
    ok "kafka-producer-perf-test already installed"
else
    info "Installing Kafka client tools..."
    KV="3.7.0"
    SV="2.13"
    wget -q "https://downloads.apache.org/kafka/${KV}/kafka_${SV}-${KV}.tgz" \
        -O /tmp/kafka.tgz
    sudo tar -xzf /tmp/kafka.tgz -C /opt/
    for bin in /opt/kafka_${SV}-${KV}/bin/*.sh; do
        sudo ln -sf "$bin" "/usr/local/bin/$(basename $bin)"
    done
    rm /tmp/kafka.tgz
    ok "kafka tools ${KV} installed"
fi

# ── 10. stress-ng ─────────────────────────────────────────────────────────────
if command -v stress-ng &>/dev/null; then
    ok "stress-ng already installed"
else
    info "Installing stress-ng..."
    sudo apt-get install -y -q stress-ng
    ok "stress-ng: $(stress-ng --version 2>&1 | head -1)"
fi

# ── Summary ───────────────────────────────────────────────────────────────────
echo -e "\n${BOLD}All benchmark tools installed:${RST}"
for tool in redis-benchmark memtier_benchmark wrk sysbench pgbench \
            mongosh cassandra-stress rabbitmq-perf-test \
            kafka-producer-perf-test.sh stress-ng; do
    command -v "$tool" &>/dev/null \
        && echo -e "  ${G}✔${RST}  $tool" \
        || echo -e "  \033[91m✘\033[0m  $tool  (check manually)"
done
echo ""
echo -e "  Next:  python3 phase1_parallel.py --smoke"
