#!/usr/bin/env bash
set -euo pipefail
AIGATE_BUILD_ROOT=$(cd "$(dirname "$0")/.." && pwd)
if (( $# > 1 )); then
  echo "usage: $0 [output-directory]" >&2
  exit 2
fi
AIGATE_BUILD_OUTPUT=${1:-"$AIGATE_BUILD_ROOT/bin"}
mkdir -p "$AIGATE_BUILD_OUTPUT"
AIGATE_BUILD_OUTPUT=$(cd "$AIGATE_BUILD_OUTPUT" && pwd)
cd "$AIGATE_BUILD_ROOT"
export GOPROXY=off GOSUMDB=off GOTOOLCHAIN=local
go version
go build -mod=vendor -trimpath -o "$AIGATE_BUILD_OUTPUT/aigate-switch" ./cmd/aigate-switch
go build -mod=vendor -trimpath -o "$AIGATE_BUILD_OUTPUT/aigate-proxy" ./cmd/aigate-proxy
go build -mod=vendor -trimpath -o "$AIGATE_BUILD_OUTPUT/aigate-replay" ./cmd/aigate-replay
"$AIGATE_BUILD_OUTPUT/aigate-switch" version
"$AIGATE_BUILD_OUTPUT/aigate-proxy" version
"$AIGATE_BUILD_OUTPUT/aigate-replay" version
