package router

import (
	"liveChatroom/message"
	"log"
)

// MessageType 消息分类枚举
type MessageType int

const (
	MessageTypeChatMessage   MessageType = iota // 聊天消息 - 需要持久化到Kafka
	MessageTypeSystemMessage                    // 系统消息 - 内存+Redis短期存储
)

// MessageClassifier 消息分类器 - 根据业务需求对消息进行分类
type MessageClassifier struct {
	// 统计信息
	chatMessagesClassified   int64
	systemMessagesClassified int64
}

// NewMessageClassifier 创建消息分类器
func NewMessageClassifier() *MessageClassifier {
	return &MessageClassifier{}
}

// ClassifyMessage 对消息进行分类
func (mc *MessageClassifier) ClassifyMessage(envelope *message.MessageEnvelope) MessageType {
	if envelope == nil || envelope.Message == nil {
		log.Printf("ClassifyMessage: nil envelope or message")
		return MessageTypeSystemMessage
	}

	switch envelope.Message.(type) {
	// 聊天消息类型 - 需要持久化到Kafka + MySQL
	case *message.MessageEnvelope_ChatMessage:
		mc.chatMessagesClassified++
		return MessageTypeChatMessage

	// 房间管理消息 - 需要持久化（属于聊天会话的一部分）
	case *message.MessageEnvelope_CreateRoom:
		mc.chatMessagesClassified++
		return MessageTypeChatMessage

	case *message.MessageEnvelope_JoinRoom:
		mc.chatMessagesClassified++
		return MessageTypeChatMessage

	case *message.MessageEnvelope_LeaveRoom:
		mc.chatMessagesClassified++
		return MessageTypeChatMessage

	case *message.MessageEnvelope_CloseRoom:
		mc.chatMessagesClassified++
		return MessageTypeChatMessage

	// 系统消息类型 - 仅内存+Redis短期存储，不持久化到Kafka
	case *message.MessageEnvelope_Heartbeat:
		mc.systemMessagesClassified++
		return MessageTypeSystemMessage

	case *message.MessageEnvelope_HeartbeatAck:
		mc.systemMessagesClassified++
		return MessageTypeSystemMessage

	case *message.MessageEnvelope_MessageFetchRequest:
		mc.systemMessagesClassified++
		return MessageTypeSystemMessage

	case *message.MessageEnvelope_MessageFetchResponse:
		mc.systemMessagesClassified++
		return MessageTypeSystemMessage

	case *message.MessageEnvelope_RoomMessageFetchRequest:
		mc.systemMessagesClassified++
		return MessageTypeSystemMessage

	case *message.MessageEnvelope_RoomMessageFetchResponse:
		mc.systemMessagesClassified++
		return MessageTypeSystemMessage

	// ACK消息 - 系统确认消息，不需要持久化
	case *message.MessageEnvelope_ChatMessageAck:
		mc.systemMessagesClassified++
		return MessageTypeSystemMessage

	case *message.MessageEnvelope_CreateRoomAck:
		mc.systemMessagesClassified++
		return MessageTypeSystemMessage

	case *message.MessageEnvelope_JoinRoomAck:
		mc.systemMessagesClassified++
		return MessageTypeSystemMessage

	case *message.MessageEnvelope_LeaveRoomAck:
		mc.systemMessagesClassified++
		return MessageTypeSystemMessage

	case *message.MessageEnvelope_CloseRoomAck:
		mc.systemMessagesClassified++
		return MessageTypeSystemMessage

	// 房间ID分配 - 系统级操作，不需要持久化
	case *message.MessageEnvelope_AllocateRoomId:
		mc.systemMessagesClassified++
		return MessageTypeSystemMessage

	case *message.MessageEnvelope_AllocateRoomIdAck:
		mc.systemMessagesClassified++
		return MessageTypeSystemMessage

	default:
		// 未知消息类型，默认为系统消息，不持久化
		log.Printf("ClassifyMessage: unknown message type: %T", envelope.Message)
		mc.systemMessagesClassified++
		return MessageTypeSystemMessage
	}
}

// ShouldPersistToKafka 判断消息是否需要持久化到Kafka
func (mc *MessageClassifier) ShouldPersistToKafka(envelope *message.MessageEnvelope) bool {
	return mc.ClassifyMessage(envelope) == MessageTypeChatMessage
}

// ShouldPersistToRedis 判断消息是否需要存储到Redis（短期缓存）
func (mc *MessageClassifier) ShouldPersistToRedis(envelope *message.MessageEnvelope) bool {
	// 所有消息都可以在Redis中短期存储（用于重复检测等）
	return true
}

// GetMessageTypeString 获取消息类型的字符串表示
func (mc *MessageClassifier) GetMessageTypeString(msgType MessageType) string {
	switch msgType {
	case MessageTypeChatMessage:
		return "CHAT_MESSAGE"
	case MessageTypeSystemMessage:
		return "SYSTEM_MESSAGE"
	default:
		return "UNKNOWN"
	}
}

// GetClassificationReason 获取分类原因（用于日志和调试）
func (mc *MessageClassifier) GetClassificationReason(envelope *message.MessageEnvelope) string {
	if envelope == nil || envelope.Message == nil {
		return "nil envelope or message"
	}

	switch envelope.Message.(type) {
	// 聊天消息类型
	case *message.MessageEnvelope_ChatMessage:
		return "user chat message - needs persistence for compliance and history"

	// 房间管理消息
	case *message.MessageEnvelope_CreateRoom:
		return "room creation - part of chat session history"

	case *message.MessageEnvelope_JoinRoom:
		return "room join - part of chat session history"

	case *message.MessageEnvelope_LeaveRoom:
		return "room leave - part of chat session history"

	case *message.MessageEnvelope_CloseRoom:
		return "room close - part of chat session history"

	// 系统消息类型
	case *message.MessageEnvelope_Heartbeat:
		return "heartbeat - temporary system signal, no persistence needed"

	case *message.MessageEnvelope_HeartbeatAck:
		return "heartbeat ack - temporary system response"

	case *message.MessageEnvelope_MessageFetchRequest:
		return "message fetch request - temporary query operation"

	case *message.MessageEnvelope_MessageFetchResponse:
		return "message fetch response - temporary query result"

	case *message.MessageEnvelope_RoomMessageFetchRequest:
		return "room message fetch request - temporary query operation"

	case *message.MessageEnvelope_RoomMessageFetchResponse:
		return "room message fetch response - temporary query result"

	// ACK消息
	case *message.MessageEnvelope_ChatMessageAck,
		*message.MessageEnvelope_CreateRoomAck,
		*message.MessageEnvelope_JoinRoomAck,
		*message.MessageEnvelope_LeaveRoomAck,
		*message.MessageEnvelope_CloseRoomAck:
		return "ack message - temporary confirmation, no persistence needed"

	// 房间ID分配
	case *message.MessageEnvelope_AllocateRoomId:
		return "room id allocation - system operation, no persistence needed"

	case *message.MessageEnvelope_AllocateRoomIdAck:
		return "room id allocation ack - system response, no persistence needed"

	default:
		return "unknown message type - default to system message"
	}
}

// GetStats 获取分类器统计信息
func (mc *MessageClassifier) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"chat_messages_classified":   mc.chatMessagesClassified,
		"system_messages_classified": mc.systemMessagesClassified,
		"total_classified":           mc.chatMessagesClassified + mc.systemMessagesClassified,
		"chat_message_ratio":         float64(mc.chatMessagesClassified) / float64(mc.chatMessagesClassified+mc.systemMessagesClassified),
	}
}

// LogClassification 记录消息分类信息（用于调试和监控）
func (mc *MessageClassifier) LogClassification(envelope *message.MessageEnvelope) {
	msgType := mc.ClassifyMessage(envelope)
	reason := mc.GetClassificationReason(envelope)

	log.Printf("MESSAGE_CLASSIFICATION: type=%s, reason=%s, msg_type=%T",
		mc.GetMessageTypeString(msgType), reason, envelope.Message)
}

// IsRoomManagementMessage 判断是否为房间管理消息
func (mc *MessageClassifier) IsRoomManagementMessage(envelope *message.MessageEnvelope) bool {
	if envelope == nil || envelope.Message == nil {
		return false
	}

	switch envelope.Message.(type) {
	case *message.MessageEnvelope_CreateRoom,
		*message.MessageEnvelope_JoinRoom,
		*message.MessageEnvelope_LeaveRoom,
		*message.MessageEnvelope_CloseRoom:
		return true
	default:
		return false
	}
}

// IsChatContentMessage 判断是否为聊天内容消息
func (mc *MessageClassifier) IsChatContentMessage(envelope *message.MessageEnvelope) bool {
	if envelope == nil || envelope.Message == nil {
		return false
	}

	switch envelope.Message.(type) {
	case *message.MessageEnvelope_ChatMessage:
		return true
	default:
		return false
	}
}

// IsSystemQueryMessage 判断是否为系统查询消息
func (mc *MessageClassifier) IsSystemQueryMessage(envelope *message.MessageEnvelope) bool {
	if envelope == nil || envelope.Message == nil {
		return false
	}

	switch envelope.Message.(type) {
	case *message.MessageEnvelope_MessageFetchRequest,
		*message.MessageEnvelope_RoomMessageFetchRequest:
		return true
	default:
		return false
	}
}

// IsHeartbeatMessage 判断是否为心跳消息
func (mc *MessageClassifier) IsHeartbeatMessage(envelope *message.MessageEnvelope) bool {
	if envelope == nil || envelope.Message == nil {
		return false
	}

	switch envelope.Message.(type) {
	case *message.MessageEnvelope_Heartbeat,
		*message.MessageEnvelope_HeartbeatAck:
		return true
	default:
		return false
	}
}
