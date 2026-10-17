package aigate

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestReplayMarkVerifyKeysFromEnv(t *testing.T) {
	a, b := strings.Repeat("a", 32), strings.Repeat("b", 32)
	spaced := " " + a + "\t"
	var eight []string
	for i := 0; i < 8; i++ {
		eight = append(eight, fmt.Sprintf("%032d", i))
	}
	array := func(keys ...string) string { return string(encodeJSON(keys)) }
	for _, tc := range []struct {
		name, legacy, extra string
		want                []string
		errContains         string
	}{
		{name: "disabled"},
		{name: "empty array", extra: "[]"},
		{name: "legacy", legacy: a, want: []string{a}},
		{name: "additional only", extra: array(a, b), want: []string{a, b}},
		{name: "union", legacy: a, extra: array(b), want: []string{a, b}},
		{name: "deduplicate", legacy: a, extra: array(a, b, b), want: []string{a, b}},
		{name: "preserve bytes", legacy: a, extra: array(spaced, "中文中文中文中文中文中文"), want: []string{a, spaced, "中文中文中文中文中文中文"}},
		{name: "limit", extra: array(eight...), want: eight},
		{name: "limit after deduplication", legacy: eight[0], extra: array(append(append([]string{}, eight...), eight...)...), want: eight},
		{name: "too many", legacy: a, extra: array(eight...), errContains: "at most 8"},
		{name: "short legacy", legacy: "SHORT_SECRET", extra: array(a), errContains: replayMarkKeyEnv},
		{name: "short member", extra: array(a, "SHORT_SECRET"), errContains: "member 2 must be at least 32 bytes"},
		{name: "empty member", extra: array(""), errContains: "member 1 must be at least 32 bytes"},
		{name: "null", extra: "null", errContains: "JSON array"},
		{name: "object", extra: `{"SECRET":true}`, errContains: "JSON array"},
		{name: "string", extra: `"SECRET"`, errContains: "JSON array"},
		{name: "number member", extra: `[42]`, errContains: "member 1 must be a string"},
		{name: "null member", extra: `[null]`, errContains: "member 1 must be a string"},
		{name: "nested member", extra: `[["SECRET"]]`, errContains: "member 1 must be a string"},
		{name: "malformed", extra: `["SECRET"`, errContains: "JSON array"},
		{name: "trailing data", extra: array(a) + " SECRET", errContains: "JSON array"},
		{name: "whitespace only", extra: " \n", errContains: "JSON array"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(replayMarkKeyEnv, tc.legacy)
			t.Setenv(replayMarkVerifyKeysEnv, tc.extra)
			keys, err := replayMarkVerifyKeysFromEnv()
			if tc.errContains != "" {
				if err == nil || !strings.Contains(err.Error(), tc.errContains) || len(keys) != 0 {
					t.Fatalf("expected %q and no keys, got %v (%d keys)", tc.errContains, err, len(keys))
				}
				for _, secret := range append([]string{a, b, "SECRET"}, eight...) {
					if strings.Contains(err.Error(), secret) {
						t.Fatal("configuration error leaked a secret")
					}
				}
				return
			}
			if err != nil || len(keys) != len(tc.want) {
				t.Fatalf("keys=%d want=%d error=%v", len(keys), len(tc.want), err)
			}
			for i := range keys {
				if string(keys[i]) != tc.want[i] {
					t.Fatalf("key %d changed", i)
				}
			}
		})
	}
}

func TestReplaySigningKeyIndependentOfVerifyKeys(t *testing.T) {
	t.Setenv(replayMarkKeyEnv, string(testMarkKey))
	t.Setenv(replayMarkVerifyKeysEnv, "invalid JSON")
	key, err := replayMarkKeyFromEnv()
	if err != nil || !bytes.Equal(key, testMarkKey) {
		t.Fatal("receiver configuration affected signing key", err)
	}
	t.Setenv(replayMarkKeyEnv, "")
	t.Setenv(replayMarkVerifyKeysEnv, string(encodeJSON([]string{string(testMarkKey)})))
	key, err = replayMarkKeyFromEnv()
	if err != nil || len(key) != 0 {
		t.Fatal("verification keys must not select a signing key", err)
	}
}

func TestReplayMarkMultipleKeysVerification(t *testing.T) {
	keys := [][]byte{testMarkKey, []byte(strings.Repeat("n", 32))}
	for i, key := range keys {
		value := replayMarkValue(key, "run-1", 7)
		if !verifyReplayMark(keys, value) {
			t.Fatalf("trusted key %d rejected", i)
		}
		for _, bad := range []string{
			"", "run-1/7", strings.Replace(value, "run-1", "run-2", 1),
			strings.Replace(value, "/7/", "/8/", 1),
			strings.Replace(value, "/7/", "/-1/", 1),
			strings.Replace(value, "/7/", "/18446744073709551616/", 1),
			value + "/extra", strings.Repeat("x", 257),
			"run-1/7/" + strings.Repeat("z", 64),
		} {
			if verifyReplayMark(keys, bad) {
				t.Fatal("accepted malformed or tampered mark")
			}
		}
	}
	if verifyReplayMark(keys, replayMarkValue([]byte(strings.Repeat("u", 32)), "run-1", 7)) {
		t.Fatal("accepted unknown signing key")
	}
}

func TestReplayVerifyKeysCheckConfig(t *testing.T) {
	dir := t.TempDir()
	config, root := filepath.Join(dir, "proxy.json"), filepath.Join(dir, "no-data")
	cfg := ProxyConfig{ServiceID: "p", RouterID: "r", Backend: &Backend{Adapter: "openai", URL: "http://unreachable.invalid/v1/chat/completions", ModelsURL: "http://unreachable.invalid/v1/models"}, Settings: json.RawMessage(`{}`)}
	if err := os.WriteFile(config, encodeJSON(cfg), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(replayMarkKeyEnv, string(testMarkKey))
	second := strings.Repeat("n", 32)
	t.Setenv(replayMarkVerifyKeysEnv, string(encodeJSON([]string{string(testMarkKey), second})))
	var out, logs bytes.Buffer
	args := []string{"--check-config", "--config", config, "--data-root", root}
	if err := RunProxy(args, &out, &logs); err != nil {
		t.Fatal(err)
	}
	if logs.String() != "replay_mark_verify_keys=2\n" || !json.Valid(out.Bytes()) {
		t.Fatalf("unexpected check output: %s / %s", out.String(), logs.String())
	}
	for _, key := range []string{string(testMarkKey), second} {
		digest := sha256.Sum256([]byte(key))
		if strings.Contains(out.String()+logs.String(), key) || strings.Contains(out.String()+logs.String(), hex.EncodeToString(digest[:])) {
			t.Fatal("check-config leaked a key or digest")
		}
	}
	t.Setenv(replayMarkVerifyKeysEnv, `["SHORT_SECRET"]`)
	for _, check := range []bool{true, false} {
		args = []string{"--config", config, "--data-root", root}
		if check {
			args = append(args, "--check-config")
		}
		out.Reset()
		logs.Reset()
		if err := RunProxy(args, &out, &logs); err == nil || !strings.Contains(err.Error(), replayMarkVerifyKeysEnv) || strings.Contains(err.Error(), "SHORT_SECRET") {
			t.Fatalf("invalid key configuration accepted or leaked: %v", err)
		}
		if out.Len() != 0 || logs.Len() != 0 {
			t.Fatal("invalid configuration produced success output")
		}
	}
	if err := RunSwitch(nil, io.Discard, io.Discard); err == nil || !strings.Contains(err.Error(), replayMarkVerifyKeysEnv) {
		t.Fatal("switch did not reject invalid verification keys before startup", err)
	}
	if _, err := os.Stat(root); !os.IsNotExist(err) {
		t.Fatal("configuration validation created storage", err)
	}
}

func TestRecordingMultipleKeysExcludeOnlyTrustedMarks(t *testing.T) {
	second := []byte(strings.Repeat("n", 32))
	f := newRecordingFixture(t, recordingDefaultMaxBytes, second)
	now := halfHour(time.Now())
	startRecording(t, f.c, "rec-keys", now, now.Add(time.Hour), nil)
	body := `{"messages":[{"role":"user","content":"hello"}]}`
	for _, mark := range []string{
		replayMarkValue(testMarkKey, "run-1", 1), replayMarkValue(second, "run-2", 1),
		replayMarkValue([]byte(strings.Repeat("u", 32)), "run-3", 1), "", "malformed",
	} {
		if code, response := postChat(t, f.switch_.URL, body, map[string]string{headerReplay: mark}); code != http.StatusOK {
			t.Fatal(code, response)
		}
	}
	if result, code := recordingCommand(t, f.c, "record-stop", map[string]any{"recording_id": "rec-keys"}); code != 200 || result.Status != "completed" {
		t.Fatal(code, result)
	}
	summary := waitRecordingEnded(t, f.c, "rec-keys")
	if summary.Requests != 3 || summary.Recorded != 3 || f.router.posts.Load() != 5 {
		t.Fatalf("recording=%+v posts=%d", summary, f.router.posts.Load())
	}
	splitControlCommand(t, f.c, "close", "quiesce", map[string]any{}, 3)
	for _, key := range [][]byte{testMarkKey, second} {
		if code, _ := postChat(t, f.switch_.URL, body, map[string]string{headerReplay: replayMarkValue(key, "run-4", 1)}); code != http.StatusServiceUnavailable {
			t.Fatalf("replay mark bypassed switch admission: %d", code)
		}
	}
	if f.router.posts.Load() != 5 {
		t.Fatal("request forwarded after switch admission closed")
	}
}
