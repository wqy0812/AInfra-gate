#!/usr/bin/env bash
set -euo pipefail
AIGATE_COVERAGE_ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$AIGATE_COVERAGE_ROOT"
go version
export GOPROXY=off GOSUMDB=off GOTOOLCHAIN=local
mkdir -p dist/coverage
# deploy/ holds ignored local release tools; cover the gateway and tracked commands.
if go test -mod=vendor -count=1 -coverprofile=dist/coverage/coverage.out "$@" . ./cmd/...; then
  AIGATE_TEST_STATUS=0
else
  AIGATE_TEST_STATUS=$?
fi
go tool cover -func=dist/coverage/coverage.out
go tool cover -html=dist/coverage/coverage.out -o dist/coverage/index.html
exit "$AIGATE_TEST_STATUS"
