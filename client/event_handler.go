package client

import (
	"liveChatroom/util/net/api"
	"liveChatroom/util/net/protocol"
	"log"
	"sync/atomic"
	"time"
)

// ClientNetworkHandler 客户端网络事件处理器 - 实现 api.ClientEventHandler 接口
type ClientNetworkHandler struct {
	client *LiveChatClient
	node   *ConnectionNode
}

// OnConnected 连接建立事件
func (h *ClientNetworkHandler) OnConnected(c *api.Client) {
	h.node.connection = c.GetConnection()
	atomic.StoreInt32(&h.node.connected, 1)
	h.node.lastPing = time.Now()

	log.Printf("Connected to server: %s", h.node.addr)

	// 如果这是当前活跃连接，标记客户端为已连接
	if h.isActiveNode() {
		atomic.StoreInt32(&h.client.connected, 1)
		if h.client.eventHandler != nil {
			h.client.eventHandler.OnConnected(h.node.addr)
		}
	}
}

// OnDisconnected 连接断开事件
func (h *ClientNetworkHandler) OnDisconnected(c *api.Client, err error) {
	log.Printf("Disconnected from server: %s", h.node.addr)
	atomic.StoreInt32(&h.node.connected, 0)
	h.node.connection = nil

	// 如果这是当前活跃连接，尝试切换到其他连接
	if h.isActiveNode() {
		atomic.StoreInt32(&h.client.connected, 0)

		if h.client.eventHandler != nil {
			h.client.eventHandler.OnDisconnected(h.node.addr, err)
		}

		// 尝试自动重连到其他节点
		go h.client.handleConnectionLoss()
	}
}

// OnReconnected 重连成功事件
func (h *ClientNetworkHandler) OnReconnected(c *api.Client, attempt int) {
	h.node.connection = c.GetConnection()
	atomic.StoreInt32(&h.node.connected, 1)
	h.node.lastPing = time.Now()

	log.Printf("Reconnected to server %s after %d attempts", h.node.addr, attempt)

	if h.isActiveNode() {
		atomic.StoreInt32(&h.client.connected, 1)
	}
}

// OnMessageReceived 接收消息事件
func (h *ClientNetworkHandler) OnMessageReceived(c *api.Client, msg protocol.Message) error {
	if err := h.client.handleIncomingMessage(msg.GetPayload()); err != nil {
		log.Printf("Failed to handle incoming message: %v", err)
		if h.client.eventHandler != nil {
			h.client.eventHandler.OnError(err)
		}
		return err
	}

	atomic.AddInt64(&h.client.stats.TotalBytes, int64(len(msg.GetPayload())))
	return nil
}

// OnHeartbeatSent 心跳发送事件
func (h *ClientNetworkHandler) OnHeartbeatSent(c *api.Client) {
	h.node.lastPing = time.Now()
}

// OnHeartbeatReceived 心跳接收事件
func (h *ClientNetworkHandler) OnHeartbeatReceived(c *api.Client) {
	// 心跳由应用层协议处理，这里只做记录
}

// OnError 错误事件
func (h *ClientNetworkHandler) OnError(c *api.Client, err error) {
	log.Printf("Connection error on %s: %v", h.node.addr, err)

	if h.client.eventHandler != nil {
		h.client.eventHandler.OnError(err)
	}

	// 如果是活跃连接出错，尝试切换
	if h.isActiveNode() {
		go h.client.handleConnectionLoss()
	}
}

// isActiveNode 检查是否为当前活跃节点
func (h *ClientNetworkHandler) isActiveNode() bool {
	activeIndex := atomic.LoadInt32(&h.client.activeConnIndex)

	h.client.connMutex.RLock()
	defer h.client.connMutex.RUnlock()

	if activeIndex >= 0 && int(activeIndex) < len(h.client.connections) {
		return h.client.connections[activeIndex] == h.node
	}

	return false
}
