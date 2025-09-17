package connection

import (
	"context"
	"fmt"
	"liveChatroom/message"
	"log"
	"sync/atomic"
	"time"
)

// MessageHandler 消息处理器 - 处理客户端发送的各种消息
type MessageHandler struct {
	server *ConnectionServer

	// 统计信息
	messagesProcessed int64
	messagesFailed    int64
	roomMessages      int64
	systemMessages    int64

	// 控制
	ctx    context.Context
	cancel context.CancelFunc
}

// NewMessageHandler 创建消息处理器
func NewMessageHandler(server *ConnectionServer) *MessageHandler {
	return &MessageHandler{
		server: server,
	}
}

// Start 启动消息处理器
func (mh *MessageHandler) Start(ctx context.Context) {
	mh.ctx, mh.cancel = context.WithCancel(ctx)
	log.Printf("MessageHandler started")
}

// Stop 停止消息处理器
func (mh *MessageHandler) Stop() {
	if mh.cancel != nil {
		mh.cancel()
	}
	log.Printf("MessageHandler stopped")
}

// HandleMessage 处理消息
func (mh *MessageHandler) HandleMessage(conn *ClientConnection, envelope *message.MessageEnvelope) error {
	atomic.AddInt64(&mh.messagesProcessed, 1)

	// 根据消息类型分发处理
	switch msg := envelope.Message.(type) {
	// 房间管理消息
	case *message.MessageEnvelope_AllocateRoomId:
		return mh.handleAllocateRoomId(conn, envelope, msg.AllocateRoomId)
	case *message.MessageEnvelope_CreateRoom:
		return mh.handleCreateRoom(conn, envelope, msg.CreateRoom)
	case *message.MessageEnvelope_JoinRoom:
		return mh.handleJoinRoom(conn, envelope, msg.JoinRoom)
	case *message.MessageEnvelope_LeaveRoom:
		return mh.handleLeaveRoom(conn, envelope, msg.LeaveRoom)
	case *message.MessageEnvelope_CloseRoom:
		return mh.handleCloseRoom(conn, envelope, msg.CloseRoom)

	// 聊天消息
	case *message.MessageEnvelope_ChatMessage:
		return mh.handleChatMessage(conn, envelope, msg.ChatMessage)

	// 消息查询
	case *message.MessageEnvelope_MessageFetchRequest:
		return mh.handleMessageFetchRequest(conn, envelope, msg.MessageFetchRequest)
	case *message.MessageEnvelope_RoomMessageFetchRequest:
		return mh.handleRoomMessageFetchRequest(conn, envelope, msg.RoomMessageFetchRequest)

	// 心跳
	case *message.MessageEnvelope_Heartbeat:
		return mh.handleHeartbeat(conn, envelope, msg.Heartbeat)

	default:
		atomic.AddInt64(&mh.messagesFailed, 1)
		return fmt.Errorf("unsupported message type: %T", envelope.Message)
	}
}

// handleAllocateRoomId 处理分配房间ID请求
func (mh *MessageHandler) handleAllocateRoomId(conn *ClientConnection, envelope *message.MessageEnvelope, req *message.AllocateRoomId) error {
	atomic.AddInt64(&mh.systemMessages, 1)

	// 转发到路由节点处理
	routerEnvelope, err := mh.createRouterMessage(conn, envelope, req)
	if err != nil {
		return mh.sendErrorResponse(conn, envelope, fmt.Sprintf("Failed to create router message: %v", err))
	}

	if err := mh.server.routerManager.SendMessageToRouter(routerEnvelope); err != nil {
		// 路由节点不可用，本地分配一个临时ID
		return mh.handleLocalRoomIdAllocation(conn, envelope)
	}

	return nil
}

// handleLocalRoomIdAllocation 本地分配房间ID（备用方案）
func (mh *MessageHandler) handleLocalRoomIdAllocation(conn *ClientConnection, envelope *message.MessageEnvelope) error {
	// 生成一个基于时间戳的房间ID
	roomID := uint64(time.Now().UnixMilli())

	ack := &message.AllocateRoomIdAck{
		RoomId: roomID,
		Status: message.AllocateRoomIdStatus_ALLOCATE_ROOM_ID_SUCCESS,
		Reason: "Allocated locally",
	}

	return mh.sendResponse(conn, envelope, ack)
}

// handleCreateRoom 处理创建房间
func (mh *MessageHandler) handleCreateRoom(conn *ClientConnection, envelope *message.MessageEnvelope, req *message.CreateRoom) error {
	atomic.AddInt64(&mh.systemMessages, 1)

	userID := conn.GetUserID()
	if userID == 0 {
		return mh.sendCreateRoomError(conn, envelope, "User not authenticated")
	}

	// 验证房间参数
	if req.RoomId == 0 {
		return mh.sendCreateRoomError(conn, envelope, "Invalid room ID")
	}
	if req.Name == "" {
		return mh.sendCreateRoomError(conn, envelope, "Room name is required")
	}
	if req.MaxMembers == 0 {
		req.MaxMembers = 100 // 默认最大成员数
	}

	// 在本地创建房间
	_, err := mh.server.roomManager.CreateRoom(req.RoomId, req.Name, userID, req.MaxMembers, req.Password)
	if err != nil {
		return mh.sendCreateRoomError(conn, envelope, err.Error())
	}

	// 更新连接的当前房间
	conn.SetCurrentRoom(req.RoomId)

	// 将连接加入房间
	if err := mh.server.roomManager.JoinRoom(req.RoomId, conn.GetID(), req.Password); err != nil {
		return mh.sendCreateRoomError(conn, envelope, fmt.Sprintf("Failed to join created room: %v", err))
	}

	// 注册连接到用户映射
	mh.server.connectionManager.UpdateUserConnection(conn)

	// 转发到路由节点
	routerEnvelope, err := mh.createRouterMessage(conn, envelope, req)
	if err != nil {
		log.Printf("Failed to forward CreateRoom to router: %v", err)
	} else {
		mh.server.routerManager.SendMessageToRouter(routerEnvelope)
	}

	// 发送成功响应
	ack := &message.CreateRoomAck{
		RoomId:      req.RoomId,
		Status:      message.CreateRoomStatus_CREATE_ROOM_SUCCESS,
		Reason:      "Room created successfully",
		RouterAddrs: mh.server.routerManager.GetRouterAddrs(),
	}

	return mh.sendResponse(conn, envelope, ack)
}

// handleJoinRoom 处理加入房间
func (mh *MessageHandler) handleJoinRoom(conn *ClientConnection, envelope *message.MessageEnvelope, req *message.JoinRoom) error {
	atomic.AddInt64(&mh.systemMessages, 1)

	userID := conn.GetUserID()
	roomID := conn.GetCurrentRoom()

	if userID == 0 {
		return mh.sendJoinRoomError(conn, envelope, "User not authenticated")
	}
	if roomID == 0 {
		return mh.sendJoinRoomError(conn, envelope, "Room ID not specified")
	}

	// 尝试加入房间
	if err := mh.server.roomManager.JoinRoom(roomID, conn.GetID(), req.Password); err != nil {
		return mh.sendJoinRoomError(conn, envelope, err.Error())
	}

	// 注册连接到用户映射
	mh.server.connectionManager.UpdateUserConnection(conn)

	// 获取房间信息
	roomInfo := mh.server.roomManager.GetRoom(roomID)
	if roomInfo == nil {
		return mh.sendJoinRoomError(conn, envelope, "Room not found after join")
	}

	// 转发到路由节点
	routerEnvelope, err := mh.createRouterMessage(conn, envelope, req)
	if err != nil {
		log.Printf("Failed to forward JoinRoom to router: %v", err)
	} else {
		mh.server.routerManager.SendMessageToRouter(routerEnvelope)
	}

	// 向房间其他成员广播用户加入消息
	joinNotification := &message.ChatMessage{
		Content:     fmt.Sprintf("User %d joined the room", userID),
		MessageType: message.MessageType_MESSAGE_TYPE_TEXT,
	}
	mh.broadcastSystemMessage(roomID, joinNotification, conn)

	// 发送成功响应
	roomInfoMsg := &message.RoomInfo{
		RoomId:         roomInfo.ID,
		Name:           roomInfo.Name,
		CreatorId:      uint64(roomInfo.CreatorID),
		MaxMembers:     roomInfo.MaxMembers,
		CurrentMembers: roomInfo.MemberCount,
		CreatedAt:      uint64(roomInfo.CreatedAt.Unix()),
		Status:         message.RoomStatus(roomInfo.Status),
		Password:       roomInfo.Password,
	}

	ack := &message.JoinRoomAck{
		RoomId:      roomID,
		Status:      message.JoinRoomStatus_JOIN_ROOM_SUCCESS,
		Reason:      "Joined room successfully",
		RoomInfo:    roomInfoMsg,
		RouterAddrs: mh.server.routerManager.GetRouterAddrs(),
	}

	return mh.sendResponse(conn, envelope, ack)
}

// handleLeaveRoom 处理离开房间
func (mh *MessageHandler) handleLeaveRoom(conn *ClientConnection, envelope *message.MessageEnvelope, req *message.LeaveRoom) error {
	atomic.AddInt64(&mh.systemMessages, 1)

	userID := conn.GetUserID()
	roomID := conn.GetCurrentRoom()

	if roomID == 0 {
		return mh.sendLeaveRoomError(conn, envelope, "Not in any room")
	}

	// 离开房间
	if err := mh.server.roomManager.LeaveRoom(roomID, conn.GetID()); err != nil {
		return mh.sendLeaveRoomError(conn, envelope, err.Error())
	}

	// 清除当前房间
	conn.SetCurrentRoom(0)

	// 转发到路由节点
	routerEnvelope, err := mh.createRouterMessage(conn, envelope, req)
	if err != nil {
		log.Printf("Failed to forward LeaveRoom to router: %v", err)
	} else {
		mh.server.routerManager.SendMessageToRouter(routerEnvelope)
	}

	// 向房间其他成员广播用户离开消息
	leaveNotification := &message.ChatMessage{
		Content:     fmt.Sprintf("User %d left the room", userID),
		MessageType: message.MessageType_MESSAGE_TYPE_TEXT,
	}
	mh.broadcastSystemMessage(roomID, leaveNotification, conn)

	// 发送成功响应
	ack := &message.LeaveRoomAck{
		RoomId: roomID,
		Status: message.LeaveRoomStatus_LEAVE_ROOM_SUCCESS,
		Reason: "Left room successfully",
	}

	return mh.sendResponse(conn, envelope, ack)
}

// handleCloseRoom 处理关闭房间
func (mh *MessageHandler) handleCloseRoom(conn *ClientConnection, envelope *message.MessageEnvelope, req *message.CloseRoom) error {
	atomic.AddInt64(&mh.systemMessages, 1)

	userID := conn.GetUserID()
	roomID := conn.GetCurrentRoom()

	if roomID == 0 {
		return mh.sendCloseRoomError(conn, envelope, "Not in any room")
	}

	// 先向房间所有成员广播关闭通知——
	// CloseRoom会清空成员列表，关房之后再广播就没人收得到了
	closeNotification := &message.ChatMessage{
		Content:     "Room has been closed by the creator",
		MessageType: message.MessageType_MESSAGE_TYPE_TEXT,
	}
	mh.broadcastSystemMessage(roomID, closeNotification, nil)

	// 关闭房间（清空成员）
	if err := mh.server.roomManager.CloseRoom(roomID, userID); err != nil {
		return mh.sendCloseRoomError(conn, envelope, err.Error())
	}

	// 转发到路由节点
	routerEnvelope, err := mh.createRouterMessage(conn, envelope, req)
	if err != nil {
		log.Printf("Failed to forward CloseRoom to router: %v", err)
	} else {
		mh.server.routerManager.SendMessageToRouter(routerEnvelope)
	}

	// 发送成功响应
	ack := &message.CloseRoomAck{
		RoomId: roomID,
		Status: message.CloseRoomStatus_CLOSE_ROOM_SUCCESS,
		Reason: "Room closed successfully",
	}

	return mh.sendResponse(conn, envelope, ack)
}

// handleChatMessage 处理聊天消息
func (mh *MessageHandler) handleChatMessage(conn *ClientConnection, envelope *message.MessageEnvelope, req *message.ChatMessage) error {
	atomic.AddInt64(&mh.roomMessages, 1)

	userID := conn.GetUserID()
	roomID := conn.GetCurrentRoom()

	if userID == 0 {
		return mh.sendChatMessageError(conn, envelope, "User not authenticated")
	}
	if roomID == 0 {
		return mh.sendChatMessageError(conn, envelope, "Not in any room")
	}

	// 验证消息内容
	if req.Content == "" {
		return mh.sendChatMessageError(conn, envelope, "Message content is empty")
	}

	// 转发到路由节点进行分发
	routerEnvelope, err := mh.createRouterMessage(conn, envelope, req)
	if err != nil {
		return mh.sendChatMessageError(conn, envelope, fmt.Sprintf("Failed to create router message: %v", err))
	}

	if err := mh.server.routerManager.SendMessageToRouter(routerEnvelope); err != nil {
		// 路由节点不可用，在本地广播
		if err := mh.server.BroadcastToRoom(roomID, envelope, nil); err != nil {
			return mh.sendChatMessageError(conn, envelope, fmt.Sprintf("Failed to broadcast message: %v", err))
		}
	}

	// 发送ACK响应（表示消息已接收，但不代表已分发）
	ack := &message.ChatMessageAck{
		RoomId: roomID,
		Status: message.ChatMessageStatus_CHAT_MESSAGE_SUCCESS,
		Reason: "Message received",
	}

	return mh.sendResponse(conn, envelope, ack)
}

// handleMessageFetchRequest 处理消息获取请求
func (mh *MessageHandler) handleMessageFetchRequest(conn *ClientConnection, envelope *message.MessageEnvelope, req *message.MessageFetchRequest) error {
	// 转发到路由节点或消息中心查询
	routerEnvelope, err := mh.createRouterMessage(conn, envelope, req)
	if err != nil {
		return mh.sendMessageFetchError(conn, envelope, fmt.Sprintf("Failed to create router message: %v", err))
	}

	if err := mh.server.routerManager.SendMessageToRouter(routerEnvelope); err != nil {
		return mh.sendMessageFetchError(conn, envelope, fmt.Sprintf("Failed to query message: %v", err))
	}

	return nil
}

// handleRoomMessageFetchRequest 处理房间消息获取请求
func (mh *MessageHandler) handleRoomMessageFetchRequest(conn *ClientConnection, envelope *message.MessageEnvelope, req *message.RoomMessageFetchRequest) error {
	userID := conn.GetUserID()
	if userID == 0 {
		return mh.sendRoomMessageFetchError(conn, envelope, "User not authenticated")
	}

	// 转发到路由节点或消息中心查询
	routerEnvelope, err := mh.createRouterMessage(conn, envelope, req)
	if err != nil {
		return mh.sendRoomMessageFetchError(conn, envelope, fmt.Sprintf("Failed to create router message: %v", err))
	}

	if err := mh.server.routerManager.SendMessageToRouter(routerEnvelope); err != nil {
		return mh.sendRoomMessageFetchError(conn, envelope, fmt.Sprintf("Failed to query room messages: %v", err))
	}

	return nil
}

// handleHeartbeat 处理心跳
func (mh *MessageHandler) handleHeartbeat(conn *ClientConnection, envelope *message.MessageEnvelope, req *message.Heartbeat) error {
	// 发送心跳响应
	ack := &message.HeartbeatAck{
		Timestamp:  req.Timestamp,
		ServerTime: uint64(time.Now().UnixMilli()),
	}

	return mh.sendResponse(conn, envelope, ack)
}

// 辅助方法

// createRouterMessage 创建发送给路由节点的消息
func (mh *MessageHandler) createRouterMessage(conn *ClientConnection, originalEnvelope *message.MessageEnvelope, messageContent interface{}) (*message.MessageEnvelope, error) {
	return mh.server.codec.CreateEnvelope(
		conn.GetUserID(),
		uint32(conn.GetCurrentRoom()),
		conn.GetLoginID(),
		mh.server.connSeqGenerator.Next(),
		messageContent,
	)
}

// broadcastSystemMessage 广播系统消息
func (mh *MessageHandler) broadcastSystemMessage(roomID uint64, chatMsg *message.ChatMessage, excludeConn *ClientConnection) {
	systemEnvelope, err := mh.server.codec.CreateEnvelope(
		0, // 系统用户
		uint32(roomID),
		0, // 系统登录ID
		mh.server.connSeqGenerator.Next(),
		chatMsg,
	)
	if err != nil {
		log.Printf("Failed to create system message: %v", err)
		return
	}

	if err := mh.server.BroadcastToRoom(roomID, systemEnvelope, excludeConn); err != nil {
		log.Printf("Failed to broadcast system message: %v", err)
	}
}

// sendResponse 发送响应消息
func (mh *MessageHandler) sendResponse(conn *ClientConnection, originalEnvelope *message.MessageEnvelope, responseMessage interface{}) error {
	responseEnvelope, err := mh.server.codec.CreateEnvelope(
		conn.GetUserID(),
		uint32(conn.GetCurrentRoom()),
		conn.GetLoginID(),
		mh.server.connSeqGenerator.Next(),
		responseMessage,
	)
	if err != nil {
		return fmt.Errorf("failed to create response envelope: %v", err)
	}

	return mh.server.SendMessageToClient(conn, responseEnvelope)
}

// sendErrorResponse 发送错误响应
func (mh *MessageHandler) sendErrorResponse(conn *ClientConnection, originalEnvelope *message.MessageEnvelope, errorMsg string) error {
	log.Printf("Sending error response: %s", errorMsg)
	atomic.AddInt64(&mh.messagesFailed, 1)
	return nil // 简化处理，实际应该发送具体的错误响应
}

// 各种错误响应方法
func (mh *MessageHandler) sendCreateRoomError(conn *ClientConnection, envelope *message.MessageEnvelope, errorMsg string) error {
	ack := &message.CreateRoomAck{
		RoomId: 0,
		Status: message.CreateRoomStatus_CREATE_ROOM_FAILED,
		Reason: errorMsg,
	}
	return mh.sendResponse(conn, envelope, ack)
}

func (mh *MessageHandler) sendJoinRoomError(conn *ClientConnection, envelope *message.MessageEnvelope, errorMsg string) error {
	ack := &message.JoinRoomAck{
		RoomId: 0,
		Status: message.JoinRoomStatus_JOIN_ROOM_FAILED,
		Reason: errorMsg,
	}
	return mh.sendResponse(conn, envelope, ack)
}

func (mh *MessageHandler) sendLeaveRoomError(conn *ClientConnection, envelope *message.MessageEnvelope, errorMsg string) error {
	ack := &message.LeaveRoomAck{
		RoomId: 0,
		Status: message.LeaveRoomStatus_LEAVE_ROOM_FAILED,
		Reason: errorMsg,
	}
	return mh.sendResponse(conn, envelope, ack)
}

func (mh *MessageHandler) sendCloseRoomError(conn *ClientConnection, envelope *message.MessageEnvelope, errorMsg string) error {
	ack := &message.CloseRoomAck{
		RoomId: 0,
		Status: message.CloseRoomStatus_CLOSE_ROOM_FAILED,
		Reason: errorMsg,
	}
	return mh.sendResponse(conn, envelope, ack)
}

func (mh *MessageHandler) sendChatMessageError(conn *ClientConnection, envelope *message.MessageEnvelope, errorMsg string) error {
	ack := &message.ChatMessageAck{
		RoomId: conn.GetCurrentRoom(),
		Status: message.ChatMessageStatus_CHAT_MESSAGE_FAILED,
		Reason: errorMsg,
	}
	return mh.sendResponse(conn, envelope, ack)
}

func (mh *MessageHandler) sendMessageFetchError(conn *ClientConnection, envelope *message.MessageEnvelope, errorMsg string) error {
	ack := &message.MessageFetchResponse{
		MessageId: envelope.MessageId,
		Exists:    false,
		Status:    message.MessageStatus_MESSAGE_STATUS_FAILED,
		Reason:    errorMsg,
	}
	return mh.sendResponse(conn, envelope, ack)
}

func (mh *MessageHandler) sendRoomMessageFetchError(conn *ClientConnection, envelope *message.MessageEnvelope, errorMsg string) error {
	ack := &message.RoomMessageFetchResponse{
		RoomId:   conn.GetCurrentRoom(),
		Messages: nil,
		Status:   message.RoomMessageFetchStatus_ROOM_MESSAGE_FETCH_FAILED,
		Reason:   errorMsg,
	}
	return mh.sendResponse(conn, envelope, ack)
}

// GetStats 获取统计信息
func (mh *MessageHandler) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"messages_processed": atomic.LoadInt64(&mh.messagesProcessed),
		"messages_failed":    atomic.LoadInt64(&mh.messagesFailed),
		"room_messages":      atomic.LoadInt64(&mh.roomMessages),
		"system_messages":    atomic.LoadInt64(&mh.systemMessages),
	}
}
