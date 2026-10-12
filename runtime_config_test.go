package aigate

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

func TestControlAddressNormalization(t *testing.T) {
	for _, target := range []string{"http://PROXY.test:80/%76%31/chat/completions", "http://proxy.test./v1/models", "http://proxy.test/other/../v1/chat/completions"} {
		if base, err := controlBase(target); err != nil || base != "http://proxy.test" {
			t.Fatalf("proxy address not normalized: %s => %s %v", target, base, err)
		}
	}
	for _, target := range []string{"ftp://bad", "http://user:key@proxy.test/v1", "http://proxy.test/v1?query=1", "http://proxy.test/v1#fragment"} {
		if _, err := controlBase(target); err == nil {
			t.Fatalf("invalid proxy URL accepted: %s", target)
		}
	}
	if base, err := controlBase("http://[::1]:8080/v1/models"); err != nil || !strings.EqualFold(base, "http://[::1]:8080") {
		t.Fatal(base, err)
	}
}
