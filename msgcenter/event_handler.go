package msgcenter

import (
	"fmt"
	"liveChatroom/util/net/net/connection"
	"liveChatroom/util/net/protocol"
	"log"
	"sync/atomic"
	"time"
)

// MessageCenterEventHandler 消息中心事件处理器 - 实现 api.EventHandler 接口
type MessageCenterEventHandler struct {
	server *MessageCenterServer
}

// NewMessageCenterEventHandler 创建消息中心事件处理器
func NewMessageCenterEventHandler(server *MessageCenterServer) *MessageCenterEventHandler {
	return &MessageCenterEventHandler{
		server: server,
	}
}

// OnConnectionAccepted 连接建立事件
func (h *MessageCenterEventHandler) OnConnectionAccepted(conn *connection.Connection) {
	// 生成连接ID
	connID := h.generateConnectionID(conn)

	// 简化实现：直接记录连接信息
	// 实际实现需要维护路由连接映射
	log.Printf("New router connection from %s", conn.RemoteAddr())

	// 更新统计信息
	atomic.AddInt64(&h.server.stats.TotalConnections, 1)
	atomic.AddInt64(&h.server.stats.ActiveConnections, 1)
	atomic.AddInt64(&h.server.stats.RouterConnections, 1)

	log.Printf("Router connected: %s from %s", connID, conn.RemoteAddr())
}

// OnConnectionClosed 连接断开事件
func (h *MessageCenterEventHandler) OnConnectionClosed(conn *connection.Connection, err error) {
	connID := h.getConnectionID(conn)
	if connID == "" {
		return
	}

	// 处理路由连接断开
	h.handleRouterDisconnect(connID)

	// 更新统计信息
	atomic.AddInt64(&h.server.stats.ActiveConnections, -1)
	atomic.AddInt64(&h.server.stats.RouterConnections, -1)

	if err != nil {
		log.Printf("Router disconnected with error: %s - %v", connID, err)
	} else {
		log.Printf("Router disconnected: %s", connID)
	}
}

// OnMessageReceived 接收消息事件
func (h *MessageCenterEventHandler) OnMessageReceived(conn *connection.Connection, msg protocol.Message) error {
	connID := h.getConnectionID(conn)
	if connID == "" {
		log.Printf("Received data from unknown connection")
		return fmt.Errorf("unknown connection")
	}

	// 获取消息数据
	data := msg.GetPayload()

	// 简化实现：直接记录消息接收
	// 实际实现需要根据具体的消息处理逻辑
	log.Printf("Received message from %s: %d bytes", connID, len(data))

	// 更新统计信息
	atomic.AddInt64(&h.server.stats.MessagesReceived, 1)

	return nil
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

// getConnectionID 获取连接ID
func (h *MessageCenterEventHandler) getConnectionID(conn *connection.Connection) string {
	// 简化实现：使用连接地址作为ID
	// 实际应该维护一个连接到ID的映射
	return fmt.Sprintf("router_%s", conn.RemoteAddr())
}

// parseRemoteAddr 解析远程地址
func (h *MessageCenterEventHandler) parseRemoteAddr(addr string) (string, int) {
	// 简化实现，解析 host:port
	// 实际实现需要更健壮的地址解析
	host := addr
	port := 0

	// 这里简化处理
	return host, port
}

// handleRouterDisconnect 处理路由连接断开
func (h *MessageCenterEventHandler) handleRouterDisconnect(connID string) {
	// 简化实现：记录断开日志
	// 实际实现需要清理路由信息和相关资源
	log.Printf("Cleaned up resources for disconnected router: %s", connID)
}
