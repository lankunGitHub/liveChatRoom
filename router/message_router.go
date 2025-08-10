package router

import (
	"context"
	"fmt"
	"liveChatroom/message"
	"log"
	"sync/atomic"
	"time"
)

// MessageRouter 消息路由器 - 负责消息分发和路由逻辑
type MessageRouter struct {
	server *RouterServer
	config *RouterConfig

	// 统计信息
	messagesRouted       int64
	messagesForwarded    int64
	messagesPersisted    int64
	messagesDropped      int64
	routingErrors        int64
	chatMessagesRouted   int64 // 聊天消息路由数
	systemMessagesRouted int64 // 系统消息路由数

	// 控制
	ctx    context.Context
	cancel context.CancelFunc
}

// NewMessageRouter 创建消息路由器
func NewMessageRouter(server *RouterServer, config *RouterConfig) *MessageRouter {
	return &MessageRouter{
		server: server,
		config: config,
	}
}

// Start 启动消息路由器
func (mr *MessageRouter) Start(ctx context.Context) {
	mr.ctx, mr.cancel = context.WithCancel(ctx)
	log.Printf("MessageRouter started")
}

// Stop 停止消息路由器
func (mr *MessageRouter) Stop() {
	if mr.cancel != nil {
		mr.cancel()
	}
	log.Printf("MessageRouter stopped")
}

// RouteMessage 路由消息
func (mr *MessageRouter) RouteMessage(fromNode *NodeConnection, envelope *message.MessageEnvelope) error {
	atomic.AddInt64(&mr.messagesRouted, 1)

	// 使用消息分类器进行分类统计
	msgType := mr.server.messageClassifier.ClassifyMessage(envelope)
	switch msgType {
	case MessageTypeChatMessage:
		atomic.AddInt64(&mr.chatMessagesRouted, 1)
	case MessageTypeSystemMessage:
		atomic.AddInt64(&mr.systemMessagesRouted, 1)
	}

	// 更新节点活动时间
	fromNode.UpdateActivity()

	// 根据消息类型进行路由
	switch msg := envelope.Message.(type) {
	// 房间管理消息
	case *message.MessageEnvelope_AllocateRoomId:
		return mr.handleAllocateRoomId(fromNode, envelope, msg.AllocateRoomId)
	case *message.MessageEnvelope_CreateRoom:
		return mr.handleCreateRoom(fromNode, envelope, msg.CreateRoom)
	case *message.MessageEnvelope_JoinRoom:
		return mr.handleJoinRoom(fromNode, envelope, msg.JoinRoom)
	case *message.MessageEnvelope_LeaveRoom:
		return mr.handleLeaveRoom(fromNode, envelope, msg.LeaveRoom)
	case *message.MessageEnvelope_CloseRoom:
		return mr.handleCloseRoom(fromNode, envelope, msg.CloseRoom)

	// 聊天消息
	case *message.MessageEnvelope_ChatMessage:
		return mr.handleChatMessage(fromNode, envelope, msg.ChatMessage)

	// 消息查询
	case *message.MessageEnvelope_MessageFetchRequest:
		return mr.handleMessageFetchRequest(fromNode, envelope, msg.MessageFetchRequest)
	case *message.MessageEnvelope_RoomMessageFetchRequest:
		return mr.handleRoomMessageFetchRequest(fromNode, envelope, msg.RoomMessageFetchRequest)

	// 心跳
	case *message.MessageEnvelope_Heartbeat:
		return mr.handleHeartbeat(fromNode, envelope, msg.Heartbeat)

	// ACK消息（可靠性相关）
	case *message.MessageEnvelope_AllocateRoomIdAck,
		*message.MessageEnvelope_CreateRoomAck,
		*message.MessageEnvelope_JoinRoomAck,
		*message.MessageEnvelope_LeaveRoomAck,
		*message.MessageEnvelope_CloseRoomAck,
		*message.MessageEnvelope_ChatMessageAck,
		*message.MessageEnvelope_MessageFetchResponse,
		*message.MessageEnvelope_RoomMessageFetchResponse,
		*message.MessageEnvelope_HeartbeatAck:
		return mr.handleACKMessage(fromNode, envelope)

	default:
		atomic.AddInt64(&mr.routingErrors, 1)
		return fmt.Errorf("unsupported message type: %T", envelope.Message)
	}
}

// handleAllocateRoomId 处理分配房间ID请求
func (mr *MessageRouter) handleAllocateRoomId(fromNode *NodeConnection, envelope *message.MessageEnvelope, req *message.AllocateRoomId) error {
	// 生成唯一房间ID
	roomID := mr.generateRoomID()

	// 在Redis中预注册房间
	if err := mr.server.redisManager.PreRegisterRoom(roomID, fromNode.GetID()); err != nil {
		return mr.sendAllocateRoomIdError(fromNode, envelope, fmt.Sprintf("Failed to pre-register room: %v", err))
	}

	// 发送成功响应
	ack := &message.AllocateRoomIdAck{
		RoomId: roomID,
		Status: message.AllocateRoomIdStatus_ALLOCATE_ROOM_ID_SUCCESS,
		Reason: "Room ID allocated successfully",
	}

	return mr.sendResponse(fromNode, envelope, ack)
}

// handleCreateRoom 处理创建房间
func (mr *MessageRouter) handleCreateRoom(fromNode *NodeConnection, envelope *message.MessageEnvelope, req *message.CreateRoom) error {
	// 提取用户和房间信息
	_, roomID, _, _, err := mr.server.codec.ExtractMessageInfo(envelope)
	if err != nil {
		return mr.sendCreateRoomError(fromNode, envelope, fmt.Sprintf("Failed to extract message info: %v", err))
	}

	// 在Redis中注册房间映射
	if err := mr.server.redisManager.RegisterRoomMapping(uint64(roomID), fromNode.GetID()); err != nil {
		return mr.sendCreateRoomError(fromNode, envelope, fmt.Sprintf("Failed to register room mapping: %v", err))
	}

	// 持久化房间创建消息到消息中心
	if err := mr.persistMessage(envelope); err != nil {
		log.Printf("Failed to persist CreateRoom message: %v", err)
		// 不阻止房间创建，只记录日志
	}

	// 发送成功响应
	ack := &message.CreateRoomAck{
		RoomId:      uint64(roomID),
		Status:      message.CreateRoomStatus_CREATE_ROOM_SUCCESS,
		Reason:      "Room created successfully",
		RouterAddrs: mr.getRouterAddresses(),
	}

	return mr.sendResponse(fromNode, envelope, ack)
}

// handleJoinRoom 处理加入房间
func (mr *MessageRouter) handleJoinRoom(fromNode *NodeConnection, envelope *message.MessageEnvelope, req *message.JoinRoom) error {
	// 提取用户和房间信息
	userID, roomID, _, _, err := mr.server.codec.ExtractMessageInfo(envelope)
	if err != nil {
		return mr.sendJoinRoomError(fromNode, envelope, fmt.Sprintf("Failed to extract message info: %v", err))
	}

	// 获取房间所在的节点
	roomNodes, err := mr.server.redisManager.GetRoomNodes(uint64(roomID))
	if err != nil {
		return mr.sendJoinRoomError(fromNode, envelope, fmt.Sprintf("Failed to get room nodes: %v", err))
	}

	// 如果房间不在当前节点上，需要添加节点映射
	nodeFound := false
	for _, nodeID := range roomNodes {
		if nodeID == fromNode.GetID() {
			nodeFound = true
			break
		}
	}

	if !nodeFound {
		if err := mr.server.redisManager.AddRoomNode(uint64(roomID), fromNode.GetID()); err != nil {
			return mr.sendJoinRoomError(fromNode, envelope, fmt.Sprintf("Failed to add room node: %v", err))
		}
	}

	// 持久化加入房间消息
	if err := mr.persistMessage(envelope); err != nil {
		log.Printf("Failed to persist JoinRoom message: %v", err)
	}

	// 向房间内其他节点广播用户加入消息
	joinNotification := &message.ChatMessage{
		Content:     fmt.Sprintf("User %d joined the room", userID),
		MessageType: message.MessageType_MESSAGE_TYPE_TEXT,
	}
	mr.broadcastToRoom(uint64(roomID), joinNotification, fromNode)

	// 发送成功响应（房间信息由连接节点填充）
	ack := &message.JoinRoomAck{
		RoomId:      uint64(roomID),
		Status:      message.JoinRoomStatus_JOIN_ROOM_SUCCESS,
		Reason:      "Joined room successfully",
		RouterAddrs: mr.getRouterAddresses(),
	}

	return mr.sendResponse(fromNode, envelope, ack)
}

// handleLeaveRoom 处理离开房间
func (mr *MessageRouter) handleLeaveRoom(fromNode *NodeConnection, envelope *message.MessageEnvelope, req *message.LeaveRoom) error {
	// 提取用户和房间信息
	userID, roomID, _, _, err := mr.server.codec.ExtractMessageInfo(envelope)
	if err != nil {
		return mr.sendLeaveRoomError(fromNode, envelope, fmt.Sprintf("Failed to extract message info: %v", err))
	}

	// 向房间内其他节点广播用户离开消息
	leaveNotification := &message.ChatMessage{
		Content:     fmt.Sprintf("User %d left the room", userID),
		MessageType: message.MessageType_MESSAGE_TYPE_TEXT,
	}
	mr.broadcastToRoom(uint64(roomID), leaveNotification, fromNode)

	// 检查房间是否为空，如果为空则清理映射
	mr.checkAndCleanupEmptyRoom(uint64(roomID), fromNode.GetID())

	// 持久化离开房间消息
	if err := mr.persistMessage(envelope); err != nil {
		log.Printf("Failed to persist LeaveRoom message: %v", err)
	}

	// 发送成功响应
	ack := &message.LeaveRoomAck{
		RoomId: uint64(roomID),
		Status: message.LeaveRoomStatus_LEAVE_ROOM_SUCCESS,
		Reason: "Left room successfully",
	}

	return mr.sendResponse(fromNode, envelope, ack)
}

// handleCloseRoom 处理关闭房间
func (mr *MessageRouter) handleCloseRoom(fromNode *NodeConnection, envelope *message.MessageEnvelope, req *message.CloseRoom) error {
	// 提取房间信息
	_, roomID, _, _, err := mr.server.codec.ExtractMessageInfo(envelope)
	if err != nil {
		return mr.sendCloseRoomError(fromNode, envelope, fmt.Sprintf("Failed to extract message info: %v", err))
	}

	// 向房间内所有节点广播关闭消息
	closeNotification := &message.ChatMessage{
		Content:     "Room has been closed by the creator",
		MessageType: message.MessageType_MESSAGE_TYPE_TEXT,
	}
	mr.broadcastToRoom(uint64(roomID), closeNotification, nil)

	// 清理房间映射
	if err := mr.server.redisManager.RemoveRoomMapping(uint64(roomID)); err != nil {
		log.Printf("Failed to remove room mapping for room %d: %v", roomID, err)
	}

	// 持久化关闭房间消息
	if err := mr.persistMessage(envelope); err != nil {
		log.Printf("Failed to persist CloseRoom message: %v", err)
	}

	// 发送成功响应
	ack := &message.CloseRoomAck{
		RoomId: uint64(roomID),
		Status: message.CloseRoomStatus_CLOSE_ROOM_SUCCESS,
		Reason: "Room closed successfully",
	}

	return mr.sendResponse(fromNode, envelope, ack)
}

// handleChatMessage 处理聊天消息
func (mr *MessageRouter) handleChatMessage(fromNode *NodeConnection, envelope *message.MessageEnvelope, req *message.ChatMessage) error {
	// 提取房间信息
	_, roomID, _, _, err := mr.server.codec.ExtractMessageInfo(envelope)
	if err != nil {
		return mr.sendChatMessageError(fromNode, envelope, fmt.Sprintf("Failed to extract message info: %v", err))
	}

	// 持久化聊天消息到消息中心
	if err := mr.persistMessage(envelope); err != nil {
		log.Printf("Failed to persist chat message: %v", err)
		// 继续分发，不因持久化失败而阻止消息传递
	}

	// 向房间内所有其他节点分发消息
	if err := mr.server.BroadcastToRoom(uint64(roomID), envelope, fromNode); err != nil {
		return mr.sendChatMessageError(fromNode, envelope, fmt.Sprintf("Failed to broadcast message: %v", err))
	}

	atomic.AddInt64(&mr.messagesForwarded, 1)

	// 发送ACK响应
	ack := &message.ChatMessageAck{
		RoomId: uint64(roomID),
		Status: message.ChatMessageStatus_CHAT_MESSAGE_SUCCESS,
		Reason: "Message delivered",
	}

	return mr.sendResponse(fromNode, envelope, ack)
}

// handleMessageFetchRequest 处理消息获取请求
func (mr *MessageRouter) handleMessageFetchRequest(fromNode *NodeConnection, envelope *message.MessageEnvelope, req *message.MessageFetchRequest) error {
	// 转发到消息中心查询
	messageCenter := mr.server.loadBalancer.SelectMessageCenter()
	if messageCenter == nil {
		return mr.sendMessageFetchError(fromNode, envelope, "No available message center")
	}

	// 转发请求到消息中心
	if err := mr.forwardToMessageCenter(messageCenter, envelope); err != nil {
		return mr.sendMessageFetchError(fromNode, envelope, fmt.Sprintf("Failed to forward request: %v", err))
	}

	return nil
}

// handleRoomMessageFetchRequest 处理房间消息获取请求
func (mr *MessageRouter) handleRoomMessageFetchRequest(fromNode *NodeConnection, envelope *message.MessageEnvelope, req *message.RoomMessageFetchRequest) error {
	// 转发到消息中心查询
	messageCenter := mr.server.loadBalancer.SelectMessageCenter()
	if messageCenter == nil {
		return mr.sendRoomMessageFetchError(fromNode, envelope, "No available message center")
	}

	// 转发请求到消息中心
	if err := mr.forwardToMessageCenter(messageCenter, envelope); err != nil {
		return mr.sendRoomMessageFetchError(fromNode, envelope, fmt.Sprintf("Failed to forward request: %v", err))
	}

	return nil
}

// handleHeartbeat 处理心跳
func (mr *MessageRouter) handleHeartbeat(fromNode *NodeConnection, envelope *message.MessageEnvelope, req *message.Heartbeat) error {
	// 更新节点心跳时间
	fromNode.UpdateHeartbeat()

	// 发送心跳响应
	ack := &message.HeartbeatAck{
		Timestamp:  req.Timestamp,
		ServerTime: uint64(time.Now().UnixMilli()),
	}

	return mr.sendResponse(fromNode, envelope, ack)
}

// handleACKMessage 处理ACK消息
func (mr *MessageRouter) handleACKMessage(fromNode *NodeConnection, envelope *message.MessageEnvelope) error {
	// 委托给可靠性管理器处理
	if mr.server.reliabilityManager != nil {
		mr.server.reliabilityManager.HandleACK(envelope)
	}
	return nil
}

// 辅助方法

// persistMessage 持久化消息到消息中心
func (mr *MessageRouter) persistMessage(envelope *message.MessageEnvelope) error {
	if err := mr.server.PersistMessage(envelope); err != nil {
		atomic.AddInt64(&mr.messagesDropped, 1)
		return err
	}
	atomic.AddInt64(&mr.messagesPersisted, 1)
	return nil
}

// forwardToMessageCenter 转发消息到消息中心
func (mr *MessageRouter) forwardToMessageCenter(center *MessageCenterConnection, envelope *message.MessageEnvelope) error {
	data, err := mr.server.codec.Serialize(envelope)
	if err != nil {
		return fmt.Errorf("failed to serialize message: %v", err)
	}

	if err := center.Send(data); err != nil {
		atomic.AddInt64(&mr.messagesDropped, 1)
		return fmt.Errorf("failed to send to message center: %v", err)
	}

	atomic.AddInt64(&mr.messagesForwarded, 1)
	return nil
}

// broadcastToRoom 向房间广播系统消息
func (mr *MessageRouter) broadcastToRoom(roomID uint64, chatMsg *message.ChatMessage, excludeNode *NodeConnection) {
	systemEnvelope, err := mr.server.codec.CreateEnvelope(
		0, // 系统用户
		uint32(roomID),
		0, // 系统登录ID
		mr.server.connSeqGenerator.Next(),
		chatMsg,
	)
	if err != nil {
		log.Printf("Failed to create system message: %v", err)
		return
	}

	if err := mr.server.BroadcastToRoom(roomID, systemEnvelope, excludeNode); err != nil {
		log.Printf("Failed to broadcast system message: %v", err)
	}
}

// checkAndCleanupEmptyRoom 检查并清理空房间
func (mr *MessageRouter) checkAndCleanupEmptyRoom(roomID uint64, nodeID string) {
	// 这里可以实现更复杂的逻辑来检查房间是否为空
	// 目前简化处理，由连接节点负责判断
}

// generateRoomID 生成房间ID
func (mr *MessageRouter) generateRoomID() uint64 {
	// 使用时间戳 + 随机数生成房间ID
	return uint64(time.Now().UnixMilli())
}

// getRouterAddresses 获取路由节点地址列表
func (mr *MessageRouter) getRouterAddresses() []string {
	// 返回当前路由节点的地址
	// 在集群环境中，这里应该返回所有路由节点的地址
	return []string{fmt.Sprintf("ws://%s:%d", mr.config.Host, mr.config.Port)}
}

// sendResponse 发送响应消息
func (mr *MessageRouter) sendResponse(node *NodeConnection, originalEnvelope *message.MessageEnvelope, responseMessage interface{}) error {
	responseEnvelope, err := mr.server.codec.CreateEnvelope(
		0, // 系统响应
		0, // 无特定房间
		0, // 系统登录ID
		mr.server.connSeqGenerator.Next(),
		responseMessage,
	)
	if err != nil {
		return fmt.Errorf("failed to create response envelope: %v", err)
	}

	return mr.server.sendMessageToNodeSync(node, responseEnvelope)
}

// 各种错误响应方法
func (mr *MessageRouter) sendAllocateRoomIdError(node *NodeConnection, envelope *message.MessageEnvelope, errorMsg string) error {
	ack := &message.AllocateRoomIdAck{
		RoomId: 0,
		Status: message.AllocateRoomIdStatus_ALLOCATE_ROOM_ID_FAILED,
		Reason: errorMsg,
	}
	return mr.sendResponse(node, envelope, ack)
}

func (mr *MessageRouter) sendCreateRoomError(node *NodeConnection, envelope *message.MessageEnvelope, errorMsg string) error {
	ack := &message.CreateRoomAck{
		RoomId: 0,
		Status: message.CreateRoomStatus_CREATE_ROOM_FAILED,
		Reason: errorMsg,
	}
	return mr.sendResponse(node, envelope, ack)
}

func (mr *MessageRouter) sendJoinRoomError(node *NodeConnection, envelope *message.MessageEnvelope, errorMsg string) error {
	ack := &message.JoinRoomAck{
		RoomId: 0,
		Status: message.JoinRoomStatus_JOIN_ROOM_FAILED,
		Reason: errorMsg,
	}
	return mr.sendResponse(node, envelope, ack)
}

func (mr *MessageRouter) sendLeaveRoomError(node *NodeConnection, envelope *message.MessageEnvelope, errorMsg string) error {
	ack := &message.LeaveRoomAck{
		RoomId: 0,
		Status: message.LeaveRoomStatus_LEAVE_ROOM_FAILED,
		Reason: errorMsg,
	}
	return mr.sendResponse(node, envelope, ack)
}

func (mr *MessageRouter) sendCloseRoomError(node *NodeConnection, envelope *message.MessageEnvelope, errorMsg string) error {
	ack := &message.CloseRoomAck{
		RoomId: 0,
		Status: message.CloseRoomStatus_CLOSE_ROOM_FAILED,
		Reason: errorMsg,
	}
	return mr.sendResponse(node, envelope, ack)
}

func (mr *MessageRouter) sendChatMessageError(node *NodeConnection, envelope *message.MessageEnvelope, errorMsg string) error {
	ack := &message.ChatMessageAck{
		RoomId: 0, // 无法确定房间ID
		Status: message.ChatMessageStatus_CHAT_MESSAGE_FAILED,
		Reason: errorMsg,
	}
	return mr.sendResponse(node, envelope, ack)
}

func (mr *MessageRouter) sendMessageFetchError(node *NodeConnection, envelope *message.MessageEnvelope, errorMsg string) error {
	ack := &message.MessageFetchResponse{
		MessageId: envelope.MessageId,
		Exists:    false,
		Status:    message.MessageStatus_MESSAGE_STATUS_FAILED,
		Reason:    errorMsg,
	}
	return mr.sendResponse(node, envelope, ack)
}

func (mr *MessageRouter) sendRoomMessageFetchError(node *NodeConnection, envelope *message.MessageEnvelope, errorMsg string) error {
	ack := &message.RoomMessageFetchResponse{
		RoomId:   0, // 无法确定房间ID
		Messages: nil,
		Status:   message.RoomMessageFetchStatus_ROOM_MESSAGE_FETCH_FAILED,
		Reason:   errorMsg,
	}
	return mr.sendResponse(node, envelope, ack)
}

// GetStats 获取统计信息
func (mr *MessageRouter) GetStats() map[string]interface{} {
	totalMessages := atomic.LoadInt64(&mr.messagesRouted)
	chatMessages := atomic.LoadInt64(&mr.chatMessagesRouted)
	systemMessages := atomic.LoadInt64(&mr.systemMessagesRouted)

	return map[string]interface{}{
		"messages_routed":        totalMessages,
		"messages_forwarded":     atomic.LoadInt64(&mr.messagesForwarded),
		"messages_persisted":     atomic.LoadInt64(&mr.messagesPersisted),
		"messages_dropped":       atomic.LoadInt64(&mr.messagesDropped),
		"routing_errors":         atomic.LoadInt64(&mr.routingErrors),
		"chat_messages_routed":   chatMessages,
		"system_messages_routed": systemMessages,
		"chat_message_ratio": func() float64 {
			if totalMessages > 0 {
				return float64(chatMessages) / float64(totalMessages)
			}
			return 0.0
		}(),
		"kafka_persistence_ratio": func() float64 {
			if totalMessages > 0 {
				return float64(chatMessages) / float64(totalMessages)
			}
			return 0.0
		}(),
	}
}
