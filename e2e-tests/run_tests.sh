#!/bin/sh
# FreeBSD CSI E2E Test Runner
#
# Run from FreeBSD storage node with kubectl access.
# Requires: kubectl, python3, zfs, ctladm
#
# Usage:
#   ./run_tests.sh                    # Run all basic tests
#   ./run_tests.sh -s                 # Include stress tests
#   ./run_tests.sh -t "test_clone"    # Run only tests matching pattern
#   ./run_tests.sh -n csi-testing     # Run in specific namespace
#   ./run_tests.sh -h                 # Show help

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# Configuration (can be overridden via environment or flags)
TEST_NAMESPACE="${TEST_NAMESPACE:-default}"
KUBECONFIG="${KUBECONFIG:-$HOME/.kube/config}"
ZFS_POOL="${ZFS_POOL:-tank}"
CSI_PREFIX="${CSI_PREFIX:-csi}"
REPORT_DIR="${REPORT_DIR:-./reports}"
STRESS="false"
TEST_PATTERN=""
VERBOSE=""
FAIL_FAST=""
LIST_TESTS=""

# Colors for output (using printf to generate actual escape sequences)
if [ -t 1 ]; then
    RED=$(printf '\033[0;31m')
    GREEN=$(printf '\033[0;32m')
    YELLOW=$(printf '\033[1;33m')
    BLUE=$(printf '\033[0;34m')
    NC=$(printf '\033[0m')
else
    RED=''
    GREEN=''
    YELLOW=''
    BLUE=''
    NC=''
fi

usage() {
    cat << EOF
FreeBSD CSI E2E Test Suite

Usage: $0 [OPTIONS]

Options:
  -n, --namespace NS      Kubernetes namespace (default: $TEST_NAMESPACE)
  -k, --kubeconfig FILE   Path to kubeconfig (default: \$KUBECONFIG)
  -p, --pool POOL         ZFS pool name (default: $ZFS_POOL)
  -s, --stress            Include stress tests
  -t, --test PATTERN      Run only tests matching pattern
  -l, --list              List available tests (don't run)
  -v, --verbose           Verbose output
  -x, --fail-fast         Stop on first failure
  -h, --help              Show this help

Environment Variables:
  TEST_NAMESPACE          Kubernetes namespace
  KUBECONFIG              Path to kubeconfig
  ZFS_POOL                ZFS pool name
  CSI_PREFIX              CSI dataset prefix (default: csi)
  REPORT_DIR              Report output directory

Examples:
  $0                              Run all basic tests
  $0 -l                           List all available tests
  $0 -l -s                        List all tests including stress tests
  $0 -s                           Include stress tests
  $0 -t test_volume               Run volume tests only
  $0 -t test_clone_chain          Run clone chain tests
  $0 -n csi-testing -s -v         Full suite, verbose, in csi-testing namespace
EOF
}

# Parse arguments
while [ $# -gt 0 ]; do
    case "$1" in
        -n|--namespace)
            TEST_NAMESPACE="$2"
            shift 2
            ;;
        -k|--kubeconfig)
            KUBECONFIG="$2"
            shift 2
            ;;
        -p|--pool)
            ZFS_POOL="$2"
            shift 2
            ;;
        -s|--stress)
            STRESS="true"
            shift
            ;;
        -t|--test)
            TEST_PATTERN="$2"
            shift 2
            ;;
        -v|--verbose)
            VERBOSE="-v"
            shift
            ;;
        -x|--fail-fast)
            FAIL_FAST="-x"
            shift
            ;;
        -l|--list)
            LIST_TESTS="true"
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "${RED}Unknown option: $1${NC}" >&2
            usage
            exit 1
            ;;
    esac
done

# Export configuration for pytest
export TEST_NAMESPACE
export KUBECONFIG
export ZFS_POOL
export CSI_PREFIX

# Header
echo "${BLUE}=======================================${NC}"
echo "${BLUE}  FreeBSD CSI E2E Test Suite${NC}"
echo "${BLUE}=======================================${NC}"
echo ""

# Show configuration
echo "Configuration:"
echo "  Namespace:  ${YELLOW}$TEST_NAMESPACE${NC}"
echo "  Kubeconfig: ${YELLOW}$KUBECONFIG${NC}"
echo "  ZFS Pool:   ${YELLOW}$ZFS_POOL${NC}"
echo "  CSI Prefix: ${YELLOW}$CSI_PREFIX${NC}"
echo "  Stress:     ${YELLOW}$STRESS${NC}"
echo ""

# Preflight checks
echo "Running preflight checks..."

check_ok() {
    echo "  ${GREEN}✓${NC} $1"
}

check_fail() {
    echo "  ${RED}✗${NC} $1"
    echo ""
    echo "${RED}ERROR: $2${NC}"
    exit 1
}

# Check kubectl
if command -v kubectl >/dev/null 2>&1; then
    check_ok "kubectl found"
else
    check_fail "kubectl" "kubectl not found in PATH"
fi

# Check cluster access
if kubectl --kubeconfig="$KUBECONFIG" cluster-info >/dev/null 2>&1; then
    check_ok "Kubernetes cluster accessible"
else
    check_fail "cluster access" "Cannot connect to Kubernetes cluster. Check KUBECONFIG."
fi

# Check CSI driver
if kubectl --kubeconfig="$KUBECONFIG" get csidriver csi.freebsd.org >/dev/null 2>&1; then
    check_ok "CSI driver csi.freebsd.org registered"
else
    check_fail "CSI driver" "CSI driver csi.freebsd.org not found. Is it installed?"
fi

# Check ZFS (uses sudo for privileged operations)
if sudo zfs list "$ZFS_POOL" >/dev/null 2>&1; then
    check_ok "ZFS pool '$ZFS_POOL' accessible (via sudo)"
else
    check_fail "ZFS" "Cannot access ZFS pool '$ZFS_POOL'. Check sudo permissions."
fi

# Check CTL (uses sudo for privileged operations)
if sudo ctladm lunlist >/dev/null 2>&1; then
    check_ok "CTL accessible (via sudo)"
else
    check_fail "CTL" "Cannot access CTL. Check sudo permissions."
fi

# Check Python3
if command -v python3 >/dev/null 2>&1; then
    PYTHON_VERSION=$(python3 --version 2>&1)
    check_ok "Python3 found ($PYTHON_VERSION)"
else
    check_fail "Python3" "python3 not found in PATH"
fi

echo ""

# Setup Python virtual environment
if [ ! -d "venv" ]; then
    echo "Creating Python virtual environment..."
    python3 -m venv venv
fi

echo "Activating virtual environment..."
. venv/bin/activate

# Install/update dependencies
echo "Installing dependencies..."
pip install -q --upgrade pip
pip install -q -r requirements.txt

# Create report directory
mkdir -p "$REPORT_DIR"

# Capture storage state before tests
echo ""
echo "Capturing initial storage state..."
# Only capture dataset names to avoid false positives from usage changes
sudo zfs list -t all -r -o name "$ZFS_POOL/$CSI_PREFIX" > "$REPORT_DIR/zfs-before.txt" 2>/dev/null || true
sudo ctladm lunlist > "$REPORT_DIR/ctl-before.txt" 2>/dev/null || true

# Build pytest arguments
PYTEST_ARGS=""

# Add verbosity if not already specified in pytest.ini
if [ -n "$VERBOSE" ]; then
    PYTEST_ARGS="$PYTEST_ARGS -vv"
fi

if [ -n "$FAIL_FAST" ]; then
    PYTEST_ARGS="$PYTEST_ARGS -x"
fi

# Test selection
if [ "$STRESS" = "false" ]; then
    PYTEST_ARGS="$PYTEST_ARGS -m 'not stress'"
fi

if [ -n "$TEST_PATTERN" ]; then
    PYTEST_ARGS="$PYTEST_ARGS -k '$TEST_PATTERN'"
fi

# Pass options to pytest
PYTEST_ARGS="$PYTEST_ARGS --namespace=$TEST_NAMESPACE"
PYTEST_ARGS="$PYTEST_ARGS --pool=$ZFS_POOL"
PYTEST_ARGS="$PYTEST_ARGS --csi-prefix=$CSI_PREFIX"

if [ -n "$KUBECONFIG" ]; then
    PYTEST_ARGS="$PYTEST_ARGS --kubeconfig=$KUBECONFIG"
fi

# List tests only (if requested)
if [ "$LIST_TESTS" = "true" ]; then
    echo ""
    echo "${BLUE}=======================================${NC}"
    echo "${BLUE}  Available Tests${NC}"
    echo "${BLUE}=======================================${NC}"
    echo ""
    eval "pytest $PYTEST_ARGS --collect-only -q"
    exit $?
fi

# Run tests
echo ""
echo "${BLUE}=======================================${NC}"
echo "${BLUE}  Starting Tests${NC}"
echo "${BLUE}=======================================${NC}"
echo ""

START_TIME=$(date +%s)

set +e
eval "pytest $PYTEST_ARGS"
TEST_EXIT=$?
set -e

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

# Capture storage state after tests
echo ""
echo "Capturing final storage state..."
# Only capture dataset names to match the before snapshot
sudo zfs list -t all -r -o name "$ZFS_POOL/$CSI_PREFIX" > "$REPORT_DIR/zfs-after.txt" 2>/dev/null || true
sudo ctladm lunlist > "$REPORT_DIR/ctl-after.txt" 2>/dev/null || true

# Summary
echo ""
echo "${BLUE}=======================================${NC}"
echo "${BLUE}  Test Summary${NC}"
echo "${BLUE}=======================================${NC}"
echo ""
echo "Duration:  ${DURATION}s"
echo "Reports:   $REPORT_DIR/"
echo "  - results.xml (JUnit XML)"
echo "  - zfs-before.txt, zfs-after.txt"
echo "  - ctl-before.txt, ctl-after.txt"
echo ""

if [ $TEST_EXIT -eq 0 ]; then
    echo "${GREEN}All tests passed!${NC}"
else
    echo "${RED}Some tests failed.${NC}"
    echo ""

    # Show storage state diff
    echo "Storage state changes:"
    if diff -q "$REPORT_DIR/zfs-before.txt" "$REPORT_DIR/zfs-after.txt" >/dev/null 2>&1; then
        echo "  ZFS: ${GREEN}No changes${NC}"
    else
        echo "  ZFS: ${YELLOW}Changed${NC}"
        echo ""
        echo "  ZFS Diff:"
        diff "$REPORT_DIR/zfs-before.txt" "$REPORT_DIR/zfs-after.txt" | head -20 || true
    fi

    if diff -q "$REPORT_DIR/ctl-before.txt" "$REPORT_DIR/ctl-after.txt" >/dev/null 2>&1; then
        echo "  CTL: ${GREEN}No changes${NC}"
    else
        echo "  CTL: ${YELLOW}Changed${NC}"
    fi
fi

exit $TEST_EXIT
