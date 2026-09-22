// TCP access boundary for a loopback Router. No HTTP routing, retries or payload changes.
package main

import (
	"flag"
	"fmt"
	"io"
	"net"
	"net/netip"
	"os"
	"strings"
	"time"
)

func main() {
	listen := flag.String("listen", "", "bound host:port")
	upstream := flag.String("upstream", "127.0.0.1:10016", "existing Router socket")
	allow := flag.String("allow", "", "comma-separated source CIDRs")
	flag.Parse()
	var prefixes []netip.Prefix
	for _, v := range strings.Split(*allow, ",") {
		p, e := netip.ParsePrefix(v)
		if e != nil {
			fmt.Fprintln(os.Stderr, "invalid source CIDR")
			os.Exit(2)
		}
		prefixes = append(prefixes, p)
	}
	l, e := net.Listen("tcp", *listen)
	if e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
	slots := make(chan struct{}, 1024)
	for {
		c, e := l.Accept()
		if e != nil {
			fmt.Fprintln(os.Stderr, e)
			os.Exit(1)
		}
		select {
		case slots <- struct{}{}:
		default:
			c.Close()
			continue
		}
		go func() {
			defer func() { <-slots }()
			forward(c, *upstream, prefixes)
		}()
	}
}

func forward(c net.Conn, upstream string, prefixes []netip.Prefix) {
	defer c.Close()
	addr := c.RemoteAddr().(*net.TCPAddr).AddrPort().Addr().Unmap()
	allowed := false
	for _, p := range prefixes {
		if p.Contains(addr) {
			allowed = true
			break
		}
	}
	if !allowed {
		return
	}
	u, e := net.DialTimeout("tcp", upstream, 5*time.Second)
	if e != nil {
		return
	}
	defer u.Close()
	done := make(chan struct{})
	go func() { io.Copy(u, c); u.(*net.TCPConn).CloseWrite(); close(done) }()
	io.Copy(c, u)
	c.(*net.TCPConn).CloseWrite()
	c.Close()
	u.Close()
	<-done
}
