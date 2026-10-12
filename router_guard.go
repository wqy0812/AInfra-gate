package aigate

import (
	"fmt"
	"net/url"
	"path"
	"strings"
)

func controlBase(s string) (string, error) {
	u, err := url.Parse(s)
	if err != nil || u.Hostname() == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return "", fmt.Errorf("Router 地址无效")
	}
	p := strings.TrimRight(path.Clean("/"+u.Path), "/")
	for _, suffix := range []string{"/chat/completions", "/models"} {
		p = strings.TrimSuffix(p, suffix)
	}
	p = strings.TrimSuffix(p, "/v1")
	host := strings.TrimRight(strings.ToLower(u.Hostname()), ".")
	port := u.Port()
	if strings.Contains(host, ":") {
		host = "[" + host + "]"
	}
	if port != "" && !(u.Scheme == "http" && port == "80") && !(u.Scheme == "https" && port == "443") {
		host += ":" + port
	}
	return strings.ToLower(u.Scheme) + "://" + host + p, nil
}
