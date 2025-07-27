package connection

import (
	"fmt"
	"liveChatroom/message"
	"liveChatroom/util/net/net/connection"
	"liveChatroom/util/net/protocol"
	"log"
	"sync/atomic"
	"time"
)

// ConnectionEventHandler 连接服务器事件处理器 - 实现 api.EventHandler 接口
type ConnectionEventHandler struct {
	server *ConnectionServer
}

// NewConnectionEventHandler 创建连接事件处理器
func NewConnectionEventHandler(server *ConnectionServer) *ConnectionEventHandler {
	return &ConnectionEventHandler{
		server: server,
	}
}

// OnConnectionAccepted 连接建立事件
func (h *ConnectionEventHandler) OnConnectionAccepted(conn *connection.Connection) {
	// 创建客户端连接封装
	connID := h.generateConnectionID(conn)
	clientConn := NewClientConnection(connID, conn)

	// 添加到连接管理器
	if err := h.server.connectionManager.AddConnection(clientConn); err != nil {
		log.Printf("Failed to add connection %s: %v", connID, err)
		conn.Close()
		return
	}

	// 更新统计信息
	atomic.AddInt64(&h.server.stats.TotalConnections, 1)
	atomic.AddInt64(&h.server.stats.ConnectionsAccepted, 1)
	atomic.AddInt64(&h.server.stats.ActiveConnections, 1)

	log.Printf("New connection established: %s from %s", connID, conn.RemoteAddr())
}

// OnConnectionClosed 连接断开事件
func (h *ConnectionEventHandler) OnConnectionClosed(conn *connection.Connection, err error) {
	connID := h.getConnectionID(conn)
	if connID == "" {
		return
	}

	// 获取客户端连接
	clientConn := h.server.connectionManager.GetConnection(connID)
	if clientConn == nil {
		return
	}

	// 处理连接断开
	h.handleConnectionDisconnect(clientConn)

	// 从连接管理器移除
	if err := h.server.connectionManager.RemoveConnection(connID); err != nil {
		log.Printf("Failed to remove connection %s: %v", connID, err)
	}

	// 更新统计信息
	atomic.AddInt64(&h.server.stats.ActiveConnections, -1)

	if err != nil {
		log.Printf("Connection closed with error: %s - %v", connID, err)
	} else {
		log.Printf("Connection closed: %s", connID)
	}
}

// OnMessageReceived 接收消息事件
func (h *ConnectionEventHandler) OnMessageReceived(conn *connection.Connection, msg protocol.Message) error {
	connID := h.getConnectionID(conn)
	if connID == "" {
		log.Printf("Received data from unknown connection")
		return fmt.Errorf("unknown connection")
	}

	// 获取客户端连接
	clientConn := h.server.connectionManager.GetConnection(connID)
	if clientConn == nil {
		log.Printf("Received data from unmanaged connection: %s", connID)
		return fmt.Errorf("unmanaged connection: %s", connID)
	}

	// 获取消息数据
	data := msg.GetPayload()

	// 处理接收到的消息
	if err := h.server.HandleMessage(clientConn, data); err != nil {
		log.Printf("Failed to handle message from %s: %v", connID, err)

		// 如果是严重错误，可能需要断开连接
		if h.isSevereError(err) {
			conn.Close()
		}
		return err
	}

	// 更新统计信息
	atomic.AddInt64(&h.server.stats.MessagesReceived, 1)
	h.server.stats.LastMessageTime = time.Now()

	return nil
}

// OnError 错误事件
func (h *ConnectionEventHandler) OnError(err error) {
	log.Printf("Server error: %v", err)
}

// 辅助方法

// generateConnectionID 生成连接ID
func (h *ConnectionEventHandler) generateConnectionID(conn *connection.Connection) string {
	// 使用远程地址和时间戳生成唯一ID
	remoteAddr := conn.RemoteAddr()
	timestamp := time.Now().UnixNano()
	return fmt.Sprintf("%s_%d", remoteAddr, timestamp)
}

// getConnectionID 获取连接ID
func (h *ConnectionEventHandler) getConnectionID(conn *connection.Connection) string {
	// 遍历连接管理器查找匹配的连接
	// 这不是最优的方法，但在这个简化实现中可以工作
	h.server.connectionManager.mutex.RLock()
	defer h.server.connectionManager.mutex.RUnlock()

	for connID, clientConn := range h.server.connectionManager.connections {
		if clientConn.GetConnection() == conn {
			return connID
		}
	}

	return ""
}

// handleConnectionDisconnect 处理连接断开
func (h *ConnectionEventHandler) handleConnectionDisconnect(clientConn *ClientConnection) {
	// 获取用户信息
	userID := clientConn.GetUserID()
	roomID := clientConn.GetCurrentRoom()

	// 如果用户在房间中，处理断开逻辑
	if roomID != 0 && userID != 0 {
		// 使用专门的断开处理器
		disconnectHandler := NewDisconnectHandler(h.server)

		if err := disconnectHandler.HandleUserDisconnect(clientConn); err != nil {
			log.Printf("Failed to handle user disconnect automatically: %v", err)

			// 如果自动处理失败，执行基础清理
			h.fallbackDisconnectCleanup(clientConn, roomID, userID)
		}
	}
}

// fallbackDisconnectCleanup 后备断开清理逻辑
func (h *ConnectionEventHandler) fallbackDisconnectCleanup(clientConn *ClientConnection, roomID uint64, userID uint32) {
	// 基础清理：直接从房间移除用户连接
	if err := h.server.roomManager.LeaveRoom(roomID, clientConn.GetID()); err != nil {
		log.Printf("Failed to remove user %d from room %d on disconnect: %v", userID, roomID, err)
	} else {
		// 向房间其他成员广播用户离线消息
		h.broadcastUserOfflineMessage(roomID, userID, clientConn)
	}
}

// broadcastUserOfflineMessage 广播用户离线消息
func (h *ConnectionEventHandler) broadcastUserOfflineMessage(roomID uint64, userID uint32, excludeConn *ClientConnection) {
	// 创建离线通知消息
	offlineMsg := &message.ChatMessage{
		Content:     fmt.Sprintf("User %d went offline", userID),
		MessageType: message.MessageType_MESSAGE_TYPE_TEXT,
	}

	// 创建系统消息信封
	systemEnvelope, err := h.server.codec.CreateEnvelope(
		0, // 系统用户
		uint32(roomID),
		0, // 系统登录ID
		h.server.connSeqGenerator.Next(),
		offlineMsg,
	)
	if err != nil {
		log.Printf("Failed to create offline notification: %v", err)
		return
	}

	// 广播给房间内其他用户
	if err := h.server.BroadcastToRoom(roomID, systemEnvelope, excludeConn); err != nil {
		log.Printf("Failed to broadcast offline notification: %v", err)
	}
}

// isSevereError 检查是否是严重错误
func (h *ConnectionEventHandler) isSevereError(err error) bool {
	// 根据错误类型判断是否需要断开连接
	// 这里可以根据具体的错误类型来判断
	return false // 简化处理
}

// isFatalError 检查是否是致命错误
func (h *ConnectionEventHandler) isFatalError(err error) bool {
	// 根据错误类型判断是否是致命错误
	// 这里可以根据具体的错误类型来判断
	return true // 简化处理，认为大部分错误都是致命的
}
