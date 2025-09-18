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
	// 消息中心由路由节点主动连接，这里入站的都视为连接节点
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
		log.Printf("Unknown connection type from %s", conn.RemoteAddr())
	}
}

// OnConnectionClosed 连接断开事件
func (h *RouterEventHandler) OnConnectionClosed(conn *connection.Connection, err error) {
	// 按连接对象反查注册的节点
	node := h.server.nodeManager.FindByConnection(conn)
	if node == nil {
		return
	}

	// 注销节点，清理表与计数（此前只减统计不删表，死节点永久残留）
	_ = h.server.nodeManager.UnregisterConnectionNode(node.GetID())

	atomic.AddInt64(&h.server.stats.ActiveConnections, -1)
	atomic.AddInt64(&h.server.stats.ConnectionNodes, -1)

	if err != nil {
		log.Printf("Node disconnected with error: %s - %v", node.GetID(), err)
	} else {
		log.Printf("Node disconnected: %s", node.GetID())
	}
}

// OnMessageReceived 接收消息事件
func (h *RouterEventHandler) OnMessageReceived(conn *connection.Connection, msg protocol.Message) error {
	// 按连接对象反查注册的节点
	node := h.server.nodeManager.FindByConnection(conn)
	if node == nil {
		log.Printf("Received data from unknown connection")
		return fmt.Errorf("unknown connection")
	}

	// 交给统一的消息处理入口（此前此处只打日志，整条路由链路不可达）
	return h.server.HandleMessage(node, msg.GetPayload())
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

// determineNodeType 确定节点类型
func (h *RouterEventHandler) determineNodeType(conn *connection.Connection) NodeType {
	// 消息中心由路由节点主动连接（NodeManager维护客户端连接），
	// 因此所有入站连接都视为连接节点
	return NodeTypeConnection
}

// parseRemoteAddr 解析远程地址
func (h *RouterEventHandler) parseRemoteAddr(addr string) (string, int) {
	// 简化实现，解析 host:port
	host := addr
	port := 0
	return host, port
}

// handleConnectionNodeDisconnect 处理连接节点断开
func (h *RouterEventHandler) handleConnectionNodeDisconnect(connID string) {
	// 简化实现：记录断开日志
	// 实际实现需要清理路由信息和通知负载均衡器
	log.Printf("Cleaned up resources for disconnected node: %s", connID)
}
