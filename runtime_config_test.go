package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestRuntimeSettingsDefaultsAndBounds(t *testing.T) {
	for _, raw := range []string{`{}`, `{"http_pool":{"max_connections_per_host":7}}`} {
		settings, err := parseRuntimeSettings([]byte(raw))
		if err != nil {
			t.Fatal(err)
		}
		if settings.TimeoutSeconds != 120 || settings.DiscoveryTimeoutSeconds != 10 || settings.ModelCacheSeconds != 60 || settings.DiscoveryRetrySeconds != 5 || settings.HTTPPool.MaxIdleConns != 256 || settings.HTTPPool.MaxIdleConnsPerHost != 128 || settings.HTTPPool.IdleTimeoutSeconds != 90 {
			t.Fatalf("runtime defaults changed: %+v", settings)
		}
		if settings.Profile.RetentionHours != 72 || settings.Profile.MaxDiskBytes != 10<<30 || settings.Profile.QueueEvents != 2048 || settings.SlowRequests.ThresholdSeconds != 300 || settings.SlowRequests.RetentionHours != 72 || settings.SlowRequests.MaxDiskBytes != 10<<30 || settings.SlowRequests.MaxQueueBytes != 64<<20 {
			t.Fatalf("observation defaults changed: %+v", settings)
		}
	}
	for _, change := range []func(*RuntimeConfig){
		func(c *RuntimeConfig) { c.RouterID = "bad/id" },
		func(c *RuntimeConfig) { c.Backend = nil },
		func(c *RuntimeConfig) { c.TimeoutSeconds = 0 },
		func(c *RuntimeConfig) { c.HTTPPool.MaxConnsPerHost = -1 },
		func(c *RuntimeConfig) { c.Backend.URL = "http://user:secret@localhost/infer" },
		func(c *RuntimeConfig) { c.Backend.Adapter = "bad" },
		func(c *RuntimeConfig) { c.Backend.UpstreamModel = json.RawMessage(`null`) },
		func(c *RuntimeConfig) { c.Backend.Compatibility.DropFields = []string{"messages"} },
		func(c *RuntimeConfig) { c.Backend.Compatibility.MetadataValues = "bad" },
		func(c *RuntimeConfig) { c.Backend.Headers = map[string]string{"X-Key": "line\nbreak"} },
		func(c *RuntimeConfig) { c.Backend.Headers = map[string]string{"invalid header": "value"} },
	} {
		cfg := fixtureConfig(t, "http://backend.invalid")
		change(cfg)
		if cfg.validate() == nil {
			t.Fatal("invalid config accepted")
		}
	}
}

func TestV2SettingsContractAndHistoricalAlias(t *testing.T) {
	for _, key := range []string{"backends", "routing", "routing_file", "management", "gateway_api_key_env", "router_id", "Backend"} {
		c := controlledFixture(t)
		s := snapshotV2("r1", "http://backend.invalid", 1)
		s.Settings = encodeJSON(map[string]any{key: nil})
		cmd := RuntimeCommand{ProtocolVersion: 2, OperationID: "prepare", GatewayID: c.id, ExpectedBootID: c.boot, Sequence: 1, Action: "prepare", Payload: encodeJSON(s)}
		code, result := controlRequestV2(t, c, cmd)
		if code != 200 || result.Error != "unknown_setting" {
			t.Fatalf("%s: %d %+v", key, code, result)
		}
	}
	for _, alias := range []string{`"old-name"`, `null`, `42`} {
		c := controlledFixture(t)
		s := snapshotV2("r1", "http://backend.invalid", 1)
		s.Settings = json.RawMessage(`{"public_model_name":` + alias + `}`)
		commandV2(t, c, "prepare", "prepare", s)
		if string(c.candidate.snapshot.Settings) != "{}" {
			t.Fatal("historical alias was retained")
		}
	}
}

func TestV2RuntimeSnapshotMapsAreIndependent(t *testing.T) {
	c := controlledFixture(t)
	s := snapshotV2("r1", "http://backend.invalid", 1)
	s.Backend.Headers = map[string]string{"Authorization": "Bearer original"}
	s.Backend.Overrides = map[string]json.RawMessage{"temperature": json.RawMessage(`0.2`)}
	s.Backend.Compatibility.DropFields = []string{"metadata"}
	commandV2(t, c, "prepare", "prepare", s)
	runtime := c.candidate.g.config.Backend
	received := c.candidate.snapshot.Backend
	// Normalization must not rewrite the received snapshot or caller-owned maps.
	if received.ModelsListPath != "" || runtime.ModelsListPath != "data" {
		t.Fatal("snapshot normalized in place")
	}
	s.Backend.Headers["Authorization"] = "changed-source"
	received.Headers["Authorization"] = "changed-snapshot"
	received.Overrides["temperature"][0] = '9'
	received.Compatibility.DropFields[0] = "tools"
	if runtime.headers.Get("Authorization") != "Bearer original" || runtime.Headers["Authorization"] != "Bearer original" || string(runtime.Overrides["temperature"]) != "0.2" || runtime.Compatibility.DropFields[0] != "metadata" {
		t.Fatal("runtime shares mutable snapshot data")
	}
}

func TestControlAddressNormalization(t *testing.T) {
	for _, target := range []string{"http://GATEWAY.test:80/%76%31/chat/completions", "http://gateway.test./v1/models", "http://gateway.test/other/../v1/chat/completions"} {
		if checkRouter(&Backend{URL: target, ModelsURL: "http://router.test/v1/models"}, []string{"http://gateway.test/v1"}) == nil {
			t.Fatalf("gateway alias accepted: %s", target)
		}
	}
	for _, target := range []string{"ftp://bad", "http://user:key@router.test/v1", "http://router.test/v1?query=1", "http://router.test/v1#fragment"} {
		if checkRouter(&Backend{URL: target, ModelsURL: "http://router.test/v1/models"}, nil) == nil {
			t.Fatalf("invalid Router URL accepted: %s", target)
		}
	}
	if checkRouter(&Backend{URL: "https://router.test/v1/chat/completions", ModelsURL: "https://GATEWAY.test:443/v1/models"}, []string{"https://gateway.test"}) == nil {
		t.Fatal("discovery loops were not guarded")
	}
	if base, err := controlBase("http://[::1]:8080/v1/models"); err != nil || !strings.EqualFold(base, "http://[::1]:8080") {
		t.Fatal(base, err)
	}
}
