package connection

import (
	"fmt"
	"liveChatroom/message"
	"log"
	"time"
)

// DisconnectHandler 断开连接处理器 - 处理用户断开时的房间清理逻辑
type DisconnectHandler struct {
	server *ConnectionServer
}

// NewDisconnectHandler 创建断开连接处理器
func NewDisconnectHandler(server *ConnectionServer) *DisconnectHandler {
	return &DisconnectHandler{
		server: server,
	}
}

// HandleUserDisconnect 处理用户断开连接
func (dh *DisconnectHandler) HandleUserDisconnect(clientConn *ClientConnection) error {
	userID := clientConn.GetUserID()
	currentRoomID := clientConn.GetCurrentRoom()

	// 如果用户不在任何房间，直接返回
	if currentRoomID == 0 {
		log.Printf("User %d disconnected, not in any room", userID)
		return nil
	}

	// 获取房间信息
	room := dh.server.roomManager.GetRoom(currentRoomID)
	if room == nil {
		log.Printf("User %d disconnected, room %d not found", userID, currentRoomID)
		return nil
	}

	// 判断用户身份并执行相应逻辑
	if dh.isRoomOwner(userID, room) {
		// 房主断开 → 关闭房间
		return dh.handleOwnerDisconnect(clientConn, room)
	} else {
		// 普通成员断开 → 离开房间
		return dh.handleMemberDisconnect(clientConn, room)
	}
}

// isRoomOwner 检查是否为房主
func (dh *DisconnectHandler) isRoomOwner(userID uint32, room *Room) bool {
	return userID == room.CreatorID
}

// handleOwnerDisconnect 处理房主断开（自动关闭房间）
func (dh *DisconnectHandler) handleOwnerDisconnect(clientConn *ClientConnection, room *Room) error {
	userID := clientConn.GetUserID()
	roomID := room.ID

	log.Printf("Room owner %d disconnected, auto-closing room %d", userID, roomID)

	// 创建关闭房间消息
	closeRoomMsg := &message.CloseRoom{
		// 房间ID从消息ID中提取，这里不需要额外字段
	}

	// 创建消息信封
	envelope, err := dh.server.codec.CreateEnvelope(
		userID,
		uint32(roomID),
		clientConn.GetLoginID(),
		dh.server.connSeqGenerator.Next(),
		closeRoomMsg,
	)
	if err != nil {
		return fmt.Errorf("failed to create close room envelope: %v", err)
	}

	// 处理关闭房间逻辑
	if err := dh.server.messageHandler.HandleMessage(clientConn, envelope); err != nil {
		log.Printf("Failed to handle auto close room: %v", err)
		return err
	}

	// 记录自动关闭事件
	dh.logAutoCloseEvent(userID, roomID, "owner_disconnect")

	return nil
}

// handleMemberDisconnect 处理普通成员断开（自动离开房间）
func (dh *DisconnectHandler) handleMemberDisconnect(clientConn *ClientConnection, room *Room) error {
	userID := clientConn.GetUserID()
	roomID := room.ID

	log.Printf("Member %d disconnected, auto-leaving room %d", userID, roomID)

	// 创建离开房间消息
	leaveRoomMsg := &message.LeaveRoom{
		// 房间ID从消息ID中提取，这里不需要额外字段
	}

	// 创建消息信封
	envelope, err := dh.server.codec.CreateEnvelope(
		userID,
		uint32(roomID),
		clientConn.GetLoginID(),
		dh.server.connSeqGenerator.Next(),
		leaveRoomMsg,
	)
	if err != nil {
		return fmt.Errorf("failed to create leave room envelope: %v", err)
	}

	// 处理离开房间逻辑
	if err := dh.server.messageHandler.HandleMessage(clientConn, envelope); err != nil {
		log.Printf("Failed to handle auto leave room: %v", err)
		return err
	}

	// 记录自动离开事件
	dh.logAutoLeaveEvent(userID, roomID, "member_disconnect")

	return nil
}

// HandleBatchDisconnect 批量处理断开连接（网络中断等场景）
func (dh *DisconnectHandler) HandleBatchDisconnect(connections []*ClientConnection) {
	log.Printf("Handling batch disconnect for %d connections", len(connections))

	// 统计房主和成员断开数量
	var ownerDisconnects []uint64 // 房间ID列表
	var memberDisconnects int

	// 分别处理每个连接
	for _, conn := range connections {
		if err := dh.HandleUserDisconnect(conn); err != nil {
			log.Printf("Failed to handle disconnect for user %d: %v", conn.GetUserID(), err)
		}

		// 统计
		if conn.GetCurrentRoom() != 0 {
			room := dh.server.roomManager.GetRoom(conn.GetCurrentRoom())
			if room != nil && dh.isRoomOwner(conn.GetUserID(), room) {
				ownerDisconnects = append(ownerDisconnects, room.ID)
			} else {
				memberDisconnects++
			}
		}
	}

	log.Printf("Batch disconnect summary: %d rooms auto-closed, %d members auto-left",
		len(ownerDisconnects), memberDisconnects)
}

// HandleGracefulDisconnect 处理优雅断开（用户主动断开）
func (dh *DisconnectHandler) HandleGracefulDisconnect(clientConn *ClientConnection) error {
	// 优雅断开时，用户可能已经主动离开房间
	// 但为了确保数据一致性，仍然执行检查
	return dh.HandleUserDisconnect(clientConn)
}

// HandleUnexpectedDisconnect 处理异常断开（网络中断、崩溃等）
func (dh *DisconnectHandler) HandleUnexpectedDisconnect(clientConn *ClientConnection) error {
	// 异常断开时，必须自动处理房间状态
	log.Printf("Unexpected disconnect detected for user %d", clientConn.GetUserID())
	return dh.HandleUserDisconnect(clientConn)
}

// 辅助方法

// logAutoCloseEvent 记录自动关闭房间事件
func (dh *DisconnectHandler) logAutoCloseEvent(userID uint32, roomID uint64, reason string) {
	log.Printf("AUTO_CLOSE_ROOM: user=%d, room=%d, reason=%s, time=%s",
		userID, roomID, reason, time.Now().Format(time.RFC3339))

	// 这里可以发送到审计日志系统
	// dh.server.auditLogger.LogRoomClose(userID, roomID, reason, true)
}

// logAutoLeaveEvent 记录自动离开房间事件
func (dh *DisconnectHandler) logAutoLeaveEvent(userID uint32, roomID uint64, reason string) {
	log.Printf("AUTO_LEAVE_ROOM: user=%d, room=%d, reason=%s, time=%s",
		userID, roomID, reason, time.Now().Format(time.RFC3339))

	// 这里可以发送到审计日志系统
	// dh.server.auditLogger.LogRoomLeave(userID, roomID, reason, true)
}

// GetDisconnectStats 获取断开连接统计信息
func (dh *DisconnectHandler) GetDisconnectStats() map[string]interface{} {
	// 这里可以返回断开连接相关的统计信息
	return map[string]interface{}{
		"handler_name":       "DisconnectHandler",
		"auto_close_rooms":   0, // 可以维护计数器
		"auto_leave_members": 0,
	}
}
