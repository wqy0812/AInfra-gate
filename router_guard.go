package main

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

func checkRouter(b *Backend, gatewayURLs []string) error {
	for _, value := range []string{b.URL, b.ModelsURL} {
		base, err := controlBase(value)
		if err != nil {
			return err
		}
		for _, gateway := range gatewayURLs {
			forbidden, _ := controlBase(gateway)
			if base == forbidden {
				return fmt.Errorf("网关只能指向 Router，禁止网关串联")
			}
		}
	}
	return nil
}
