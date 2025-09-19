package msgcenter

import (
	"fmt"
	"liveChatroom/util/net/net/connection"
	"liveChatroom/util/net/protocol"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// MessageCenterEventHandler 消息中心事件处理器 - 实现 api.EventHandler 接口
type MessageCenterEventHandler struct {
	server *MessageCenterServer

	// 底层连接 -> 路由连接封装的映射
	routerConns map[*connection.Connection]*RouterConnection
	mutex       sync.RWMutex
}

// NewMessageCenterEventHandler 创建消息中心事件处理器
func NewMessageCenterEventHandler(server *MessageCenterServer) *MessageCenterEventHandler {
	return &MessageCenterEventHandler{
		server:      server,
		routerConns: make(map[*connection.Connection]*RouterConnection),
	}
}

// OnConnectionAccepted 连接建立事件
func (h *MessageCenterEventHandler) OnConnectionAccepted(conn *connection.Connection) {
	// 生成连接ID并创建路由连接封装（此前RouterConnection从不被构造，
	// 导致响应发送路径空指针）
	connID := h.generateConnectionID(conn)
	routerConn := NewRouterConnection(connID, conn)

	h.mutex.Lock()
	h.routerConns[conn] = routerConn
	h.mutex.Unlock()

	// 更新统计信息
	atomic.AddInt64(&h.server.stats.TotalConnections, 1)
	atomic.AddInt64(&h.server.stats.ActiveConnections, 1)
	atomic.AddInt64(&h.server.stats.RouterConnections, 1)

	log.Printf("Router connected: %s from %s", connID, conn.RemoteAddr())
}

// OnConnectionClosed 连接断开事件
func (h *MessageCenterEventHandler) OnConnectionClosed(conn *connection.Connection, err error) {
	h.mutex.Lock()
	routerConn, exists := h.routerConns[conn]
	if exists {
		delete(h.routerConns, conn)
	}
	h.mutex.Unlock()

	if !exists {
		return
	}

	// 关闭路由连接封装
	routerConn.Close()

	// 更新统计信息
	atomic.AddInt64(&h.server.stats.ActiveConnections, -1)
	atomic.AddInt64(&h.server.stats.RouterConnections, -1)

	if err != nil {
		log.Printf("Router disconnected with error: %s - %v", routerConn.GetID(), err)
	} else {
		log.Printf("Router disconnected: %s", routerConn.GetID())
	}
}

// OnMessageReceived 接收消息事件
func (h *MessageCenterEventHandler) OnMessageReceived(conn *connection.Connection, msg protocol.Message) error {
	h.mutex.RLock()
	routerConn := h.routerConns[conn]
	h.mutex.RUnlock()

	if routerConn == nil {
		log.Printf("Received data from unknown connection")
		return fmt.Errorf("unknown connection")
	}

	// 交给统一的消息处理入口（此前此处只打日志，整条持久化链路不可达）
	return h.server.HandleMessage(routerConn, msg.GetPayload())
}

// OnError 错误事件
func (h *MessageCenterEventHandler) OnError(err error) {
	log.Printf("Message center server error: %v", err)
}

// 辅助方法

// generateConnectionID 生成连接ID
func (h *MessageCenterEventHandler) generateConnectionID(conn *connection.Connection) string {
	// 使用远程地址和时间戳生成唯一ID
	remoteAddr := conn.RemoteAddr()
	timestamp := time.Now().UnixNano()
	return fmt.Sprintf("msgcenter_%s_%d", remoteAddr, timestamp)
}

// parseRemoteAddr 解析远程地址
func (h *MessageCenterEventHandler) parseRemoteAddr(addr string) (string, int) {
	// 简化实现，解析 host:port
	host := addr
	port := 0
	return host, port
}
