package main

import (
	"bytes"
	"io"
	"net"
	"net/netip"
	"testing"
	"time"
)

func TestRestrictedTransparentRouterAccess(t *testing.T) {
	upstream, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	defer upstream.Close()
	reached := make(chan []byte, 1)
	go func() {
		c, e := upstream.Accept()
		if e != nil {
			return
		}
		defer c.Close()
		body, _ := io.ReadAll(c)
		reached <- body
		c.Write(body)
	}()
	for _, allowed := range []bool{false, true} {
		listener, e := net.Listen("tcp", "127.0.0.1:0")
		if e != nil {
			t.Fatal(e)
		}
		prefix := "192.0.2.0/24"
		if allowed {
			prefix = "127.0.0.1/32"
		}
		done := make(chan struct{})
		go func() {
			defer close(done)
			c, e := listener.Accept()
			if e == nil {
				forward(c, upstream.Addr().String(), []netip.Prefix{netip.MustParsePrefix(prefix)})
			}
		}()
		c, e := net.Dial("tcp", listener.Addr().String())
		if e != nil {
			t.Fatal(e)
		}
		c.SetDeadline(time.Now().Add(3 * time.Second))
		body := bytes.Repeat([]byte("data: {\"content\":\"原样传输\"}\n\n"), 1024)
		c.Write(body)
		c.(*net.TCPConn).CloseWrite()
		got, _ := io.ReadAll(c)
		c.Close()
		listener.Close()
		<-done
		if allowed {
			if !bytes.Equal(body, got) || !bytes.Equal(body, <-reached) {
				t.Fatal("payload or stream truncated")
			}
		} else {
			if len(got) != 0 || len(reached) != 0 {
				t.Fatal("unauthorized source reached Router")
			}
		}
	}
}
