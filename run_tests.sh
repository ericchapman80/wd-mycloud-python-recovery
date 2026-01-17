#!/bin/bash
# Run tests for the project
# Usage:
#   ./run_tests.sh           - Run all tests for all modules with coverage
#   ./run_tests.sh restsdk   - Run only restsdk_public.py tests
#   ./run_tests.sh html      - Generate HTML coverage report

set -e  # Exit on error

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[0;33m'
NC='\033[0m' # No Color

MODE="${1:-all}"

case "$MODE" in
  restsdk)
    echo -e "${BLUE}Running tests for restsdk_public.py only...${NC}"
    pytest \
      tests/test_restsdk_high_value.py \
      tests/test_restsdk_core_functions.py \
      tests/test_restsdk_public.py \
      tests/test_db_flows.py \
      --cov=restsdk_public \
      --cov-report=term-missing \
      -v
    ;;
  
  html)
    echo -e "${BLUE}Running all tests and generating HTML coverage report...${NC}"
    pytest tests/ \
      --cov=. \
      --cov-report=html \
      --cov-report=term \
      -v
    echo ""
    echo -e "${GREEN}✓ HTML coverage report generated in htmlcov/index.html${NC}"
    echo "Open with: open htmlcov/index.html"
    ;;
  
  all|*)
    echo -e "${BLUE}Running all unit tests for all modules...${NC}"
    echo ""
    pytest tests/ \
      --cov=. \
      --cov-report=term-missing \
      -v
    ;;
esac

echo ""
echo -e "${GREEN}✓ Tests completed${NC}"
echo ""
echo -e "${YELLOW}Usage:${NC}"
echo "  ./run_tests.sh           - Run all tests (default)"
echo "  ./run_tests.sh restsdk   - Run only restsdk_public.py tests"
echo "  ./run_tests.sh html      - Generate HTML coverage report"
