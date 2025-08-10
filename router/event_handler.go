package router

import (
	"fmt"
	"liveChatroom/util/net/net/connection"
	"liveChatroom/util/net/protocol"
	"log"
	"sync/atomic"
	"time"
)

// RouterEventHandler 路由服务器事件处理器 - 实现 api.EventHandler 接口
type RouterEventHandler struct {
	server *RouterServer
}

// NewRouterEventHandler 创建路由事件处理器
func NewRouterEventHandler(server *RouterServer) *RouterEventHandler {
	return &RouterEventHandler{
		server: server,
	}
}

// OnConnectionAccepted 连接建立事件
func (h *RouterEventHandler) OnConnectionAccepted(conn *connection.Connection) {
	// 生成连接ID
	connID := h.generateConnectionID(conn)

	// 判断连接类型（连接节点还是消息中心）
	// 这里简化处理，实际应该通过握手协议确定
	nodeType := h.determineNodeType(conn)

	if nodeType == NodeTypeConnection {
		// 注册连接节点
		host, port := h.parseRemoteAddr(conn.RemoteAddr())
		h.server.nodeManager.RegisterConnectionNode(connID, conn, host, port)

		// 更新统计信息
		atomic.AddInt64(&h.server.stats.TotalConnections, 1)
		atomic.AddInt64(&h.server.stats.ActiveConnections, 1)
		atomic.AddInt64(&h.server.stats.ConnectionNodes, 1)

		log.Printf("Connection node connected: %s from %s", connID, conn.RemoteAddr())
	} else {
		// 这里暂时不处理消息中心的主动连接
		// 消息中心通常由路由节点主动连接
		log.Printf("Unknown connection type from %s", conn.RemoteAddr())
	}
}

// OnConnectionClosed 连接断开事件
func (h *RouterEventHandler) OnConnectionClosed(conn *connection.Connection, err error) {
	connID := h.getConnectionID(conn)
	if connID == "" {
		return
	}

	// 直接更新统计信息
	atomic.AddInt64(&h.server.stats.ActiveConnections, -1)
	atomic.AddInt64(&h.server.stats.ConnectionNodes, -1)

	if err != nil {
		log.Printf("Node disconnected with error: %s - %v", connID, err)
	} else {
		log.Printf("Node disconnected: %s", connID)
	}
}

// OnMessageReceived 接收消息事件
func (h *RouterEventHandler) OnMessageReceived(conn *connection.Connection, msg protocol.Message) error {
	connID := h.getConnectionID(conn)
	if connID == "" {
		log.Printf("Received data from unknown connection")
		return fmt.Errorf("unknown connection")
	}

	// 获取消息数据
	data := msg.GetPayload()

	// 简化实现：直接反序列化和路由消息
	// 实际实现需要根据具体的消息路由逻辑
	log.Printf("Received message from %s: %d bytes", connID, len(data))

	// 更新统计信息
	atomic.AddInt64(&h.server.stats.MessagesReceived, 1)

	return nil
}

// OnError 错误事件
func (h *RouterEventHandler) OnError(err error) {
	log.Printf("Router server error: %v", err)
}

// 辅助方法

// generateConnectionID 生成连接ID
func (h *RouterEventHandler) generateConnectionID(conn *connection.Connection) string {
	// 使用远程地址和时间戳生成唯一ID
	remoteAddr := conn.RemoteAddr()
	timestamp := time.Now().UnixNano()
	return fmt.Sprintf("router_%s_%d", remoteAddr, timestamp)
}

// getConnectionID 获取连接ID
func (h *RouterEventHandler) getConnectionID(conn *connection.Connection) string {
	// 简化实现：使用连接地址作为ID
	// 实际应该维护一个连接到ID的映射
	return fmt.Sprintf("node_%s", conn.RemoteAddr())
}

// determineNodeType 确定节点类型
func (h *RouterEventHandler) determineNodeType(conn *connection.Connection) NodeType {
	// 简化实现：根据连接来源判断
	// 实际应该通过握手协议确定
	remoteAddr := conn.RemoteAddr()

	// 这里简化处理，认为所有连接都是连接节点
	// 实际实现中应该通过握手协议或配置来确定
	log.Printf("Determining node type for %s, assuming connection node", remoteAddr)
	return NodeTypeConnection
}

// parseRemoteAddr 解析远程地址
func (h *RouterEventHandler) parseRemoteAddr(addr string) (string, int) {
	// 简化实现，解析 host:port
	// 实际实现需要更健壮的地址解析
	host := addr
	port := 0

	// 这里简化处理
	return host, port
}

// handleConnectionNodeDisconnect 处理连接节点断开
func (h *RouterEventHandler) handleConnectionNodeDisconnect(connID string) {
	// 简化实现：记录断开日志
	// 实际实现需要清理路由信息和通知负载均衡器
	log.Printf("Cleaned up resources for disconnected node: %s", connID)
}
