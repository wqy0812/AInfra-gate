package aigate

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Replay requests carry "<run_id>/<seq>/<hmac>" signed with the deployment
// replay mark key. A verified mark only skips switch recording and labels proxy
// observations traffic=replay; it never grants admission, routing or a lease.
const (
	headerReplay            = "X-Aigate-Replay"
	replayMarkKeyEnv        = "AIGATE_REPLAY_MARK_KEY"
	replayMarkVerifyKeysEnv = "AIGATE_REPLAY_MARK_VERIFY_KEYS"
	replayMarkMinBytes      = 32
	replayMarkMaxVerifyKeys = 8
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

// Receivers accept the union of the legacy key and the additional verification
// keys. Replay senders deliberately keep using replayMarkKeyFromEnv alone.
func replayMarkVerifyKeysFromEnv() ([][]byte, error) {
	key, err := replayMarkKeyFromEnv()
	if err != nil {
		return nil, err
	}
	var keys [][]byte
	seen := make(map[string]bool)
	add := func(key string) {
		if !seen[key] {
			seen[key] = true
			keys = append(keys, []byte(key))
		}
	}
	if len(key) != 0 {
		add(string(key))
	}
	if raw := os.Getenv(replayMarkVerifyKeysEnv); raw != "" {
		var members []json.RawMessage
		if json.Unmarshal([]byte(raw), &members) != nil || members == nil {
			return nil, fmt.Errorf("%s must be a JSON array of strings", replayMarkVerifyKeysEnv)
		}
		for i, member := range members {
			var key *string
			if json.Unmarshal(member, &key) != nil || key == nil {
				return nil, fmt.Errorf("%s member %d must be a string", replayMarkVerifyKeysEnv, i+1)
			}
			if len(*key) < replayMarkMinBytes {
				return nil, fmt.Errorf("%s member %d must be at least %d bytes", replayMarkVerifyKeysEnv, i+1, replayMarkMinBytes)
			}
			add(*key)
			if len(keys) > replayMarkMaxVerifyKeys {
				return nil, fmt.Errorf("%s and %s allow at most %d distinct verification keys", replayMarkKeyEnv, replayMarkVerifyKeysEnv, replayMarkMaxVerifyKeys)
			}
		}
	}
	return keys, nil
}

func replayMarkSignature(key []byte, runID string, seq uint64) string {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte("aigate-replay-v1\n" + runID + "/" + strconv.FormatUint(seq, 10)))
	return hex.EncodeToString(mac.Sum(nil))
}

func replayMarkValue(key []byte, runID string, seq uint64) string {
	return runID + "/" + strconv.FormatUint(seq, 10) + "/" + replayMarkSignature(key, runID, seq)
}

// verifyReplayMark is false for missing keys, a malformed value or a bad
// signature; callers then treat the request as ordinary business traffic.
func verifyReplayMark(keys [][]byte, value string) bool {
	if len(keys) == 0 || value == "" || len(value) > 256 {
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
	matched := false
	for _, key := range keys {
		if len(key) >= replayMarkMinBytes && hmac.Equal([]byte(parts[2]), []byte(replayMarkSignature(key, parts[0], seq))) {
			matched = true
		}
	}
	return matched
}
