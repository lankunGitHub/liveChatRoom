package http

import (
	"liveChatroom/util/net/base/buffer"
	"testing"
)

func TestParseSimpleRequest(t *testing.T) {
	p := NewHTTPParser()
	buf := buffer.Get(8192)
	defer buffer.Put(buf)
	buf.Write([]byte("GET /a HTTP/1.1\r\nHost: x\r\n\r\n"))
	msgs, err := p.Parse(buf)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}
	if msgs[0].GetType() != "http_request" {
		t.Fatalf("expected http_request, got %s", msgs[0].GetType())
	}
}

func TestParseWithBody(t *testing.T) {
	p := NewHTTPParser()
	buf := buffer.Get(8192)
	defer buffer.Put(buf)
	buf.Write([]byte("POST /b HTTP/1.1\r\nHost: x\r\nContent-Length: 5\r\n\r\nhello"))
	msgs, err := p.Parse(buf)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	if len(msgs) != 1 || string(msgs[0].GetPayload()) != "hello" {
		t.Fatalf("expected body hello, got %d msgs", len(msgs))
	}
}

func TestParsePipelined(t *testing.T) {
	p := NewHTTPParser()
	buf := buffer.Get(8192)
	defer buffer.Put(buf)
	buf.Write([]byte("GET /a HTTP/1.1\r\nHost: x\r\n\r\nGET /b HTTP/1.1\r\nHost: x\r\n\r\n"))
	msgs, err := p.Parse(buf)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	if len(msgs) != 2 {
		t.Fatalf("expected 2 pipelined messages, got %d", len(msgs))
	}
}

func TestParseChunked(t *testing.T) {
	p := NewHTTPParser()
	buf := buffer.Get(8192)
	defer buffer.Put(buf)
	buf.Write([]byte("POST /c HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\n\r\n"))
	msgs, err := p.Parse(buf)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	if len(msgs) != 1 || string(msgs[0].GetPayload()) != "hello" {
		t.Fatalf("expected chunked body hello, got %d msgs", len(msgs))
	}
}
