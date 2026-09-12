#!/bin/sh
set -eu
# Restore the matching previous Go binary/config/v1 state, preserving current state.
exec sh /data4/aigate/backups/20260912T131100Z-upgrade/rollback.sh
