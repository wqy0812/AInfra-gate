package aigate

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Replay requests carry "<run_id>/<seq>/<hmac>" signed with the deployment
// replay mark key. A verified mark only skips switch recording and labels proxy
// observations traffic=replay; it never grants admission, routing or a lease.
const (
	headerReplay       = "X-Aigate-Replay"
	replayMarkKeyEnv   = "AIGATE_REPLAY_MARK_KEY"
	replayMarkMinBytes = 32
)

func replayMarkKeyFromEnv() ([]byte, error) {
	key := os.Getenv(replayMarkKeyEnv)
	if key == "" {
		return nil, nil
	}
	if len(key) < replayMarkMinBytes {
		return nil, fmt.Errorf("%s must be at least %d bytes", replayMarkKeyEnv, replayMarkMinBytes)
	}
	return []byte(key), nil
}

func replayMarkSignature(key []byte, runID string, seq uint64) string {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte("aigate-replay-v1\n" + runID + "/" + strconv.FormatUint(seq, 10)))
	return hex.EncodeToString(mac.Sum(nil))
}

func replayMarkValue(key []byte, runID string, seq uint64) string {
	return runID + "/" + strconv.FormatUint(seq, 10) + "/" + replayMarkSignature(key, runID, seq)
}

// verifyReplayMark is false for a missing key, a malformed value or a bad
// signature; callers then treat the request as ordinary business traffic.
func verifyReplayMark(key []byte, value string) bool {
	if len(key) == 0 || value == "" || len(value) > 256 {
		return false
	}
	parts := strings.Split(value, "/")
	if len(parts) != 3 || !validOperationID(parts[0]) || len(parts[2]) != 64 {
		return false
	}
	seq, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return false
	}
	return hmac.Equal([]byte(parts[2]), []byte(replayMarkSignature(key, parts[0], seq)))
}
