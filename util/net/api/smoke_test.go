package api

import (
	"fmt"
	"liveChatroom/util/net/net/connection"
	"liveChatroom/util/net/protocol"
	"sync"
	"testing"
	"time"
)

// TestEndToEndSmoke 端到端冒烟：server + client 真实socket往返
// 覆盖：listener accept、epoll ABI修复、ET读排空、协议检测、
// HTTP解析器修复、客户端读循环
func TestEndToEndSmoke(t *testing.T) {
	const addr = "127.0.0.1:19091"

	// 服务端
	var mu sync.Mutex
	var received []string
	srv, err := NewServer(&ServerConfig{
		Address:        addr,
		ReactorCount:   2,
		WorkerCount:    4,
		ReadBufferSize: 8192, WriteBufferSize: 8192,
		MaxConnections: 100,
	}, &smokeHandler{
		onMsg: func(conn *connection.Connection, msg protocol.Message) error {
			mu.Lock()
			received = append(received, string(msg.GetPayload()))
			mu.Unlock()
			// 回显
			conn.WriteMessage(msg)
			return nil
		},
	})
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}
	if err := srv.Start(); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}
	defer srv.Stop()
	time.Sleep(200 * time.Millisecond) // 等listener就绪

	// 客户端
	var mu2 sync.Mutex
	var echoed []string
	cli, err := NewClient(DefaultClientConfig(), &smokeClientHandler{
		onMsg: func(c *Client, msg protocol.Message) error {
			mu2.Lock()
			echoed = append(echoed, string(msg.GetPayload()))
			mu2.Unlock()
			return nil
		},
		onErr: func(c *Client, err error) { t.Logf("client error: %v", err) },
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	if err := cli.Connect(addr); err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer cli.Disconnect()

	// 非阻塞connect立即返回，等握手完成
	time.Sleep(300 * time.Millisecond)

	// 发送3条带body的POST请求（客户端发送、服务端回显、客户端读回）
	bodies := []string{"hello", "world", "chat"}
	for i, body := range bodies {
		req := fmt.Sprintf("POST /echo%d HTTP/1.1\r\nHost: x\r\nContent-Length: %d\r\n\r\n%s", i, len(body), body)
		if err := cli.Send([]byte(req)); err != nil {
			t.Fatalf("send %d failed: %v", i, err)
		}
	}

	// 等待往返
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n1 := len(received)
		mu.Unlock()
		mu2.Lock()
		n2 := len(echoed)
		mu2.Unlock()
		if n1 >= 3 && n2 >= 3 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	mu.Lock()
	n1 := len(received)
	mu.Unlock()
	mu2.Lock()
	n2 := len(echoed)
	mu2.Unlock()

	if n1 != 3 {
		t.Fatalf("server received %d messages, want 3", n1)
	}
	if n2 != 3 {
		t.Fatalf("client received %d echoes, want 3", n2)
	}
	// 校验回显内容
	mu2.Lock()
	payloads := append([]string{}, echoed...)
	mu2.Unlock()
	for i, body := range bodies {
		if payloads[i] != body {
			t.Fatalf("echo %d mismatch: got %q, want %q", i, payloads[i], body)
		}
	}
	t.Logf("end-to-end smoke OK: server received %d, client echoed %d", n1, n2)
}

type smokeHandler struct {
	onMsg func(*connection.Connection, protocol.Message) error
	onAcc func(*connection.Connection)
}

func (h *smokeHandler) OnConnectionAccepted(conn *connection.Connection) {
	if h.onAcc != nil {
		h.onAcc(conn)
	}
}
func (h *smokeHandler) OnConnectionClosed(conn *connection.Connection, err error) {
	if err != nil {
		fmt.Printf("SERVER CONN CLOSED with err: %v\n", err)
	}
}
func (h *smokeHandler) OnMessageReceived(conn *connection.Connection, msg protocol.Message) error {
	if h.onMsg != nil {
		return h.onMsg(conn, msg)
	}
	return nil
}
func (h *smokeHandler) OnError(err error) {
	fmt.Printf("SERVER ERROR: %v\n", err)
}

type smokeClientHandler struct {
	onMsg func(*Client, protocol.Message) error
	onErr func(*Client, error)
}

func (h *smokeClientHandler) OnConnected(c *Client) {}
func (h *smokeClientHandler) OnDisconnected(c *Client, err error) {
	fmt.Printf("CLIENT DISCONNECTED: %v\n", err)
}
func (h *smokeClientHandler) OnReconnected(c *Client, attempt int) {}
func (h *smokeClientHandler) OnHeartbeatSent(c *Client)            {}
func (h *smokeClientHandler) OnHeartbeatReceived(c *Client)        {}
func (h *smokeClientHandler) OnError(c *Client, err error) {
	if h.onErr != nil {
		h.onErr(c, err)
	}
}
func (h *smokeClientHandler) OnMessageReceived(c *Client, msg protocol.Message) error {
	if h.onMsg != nil {
		return h.onMsg(c, msg)
	}
	return nil
}
