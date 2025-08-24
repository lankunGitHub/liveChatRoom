package client

import (
	"fmt"
	"liveChatroom/message"
	"log"
	"time"
)

// ExampleEventHandler 示例事件处理器
type ExampleEventHandler struct {
	clientID string
}

// NewExampleEventHandler 创建示例事件处理器
func NewExampleEventHandler(clientID string) *ExampleEventHandler {
	return &ExampleEventHandler{
		clientID: clientID,
	}
}

// 连接事件
func (h *ExampleEventHandler) OnConnected(serverAddr string) {
	log.Printf("[%s] Connected to server: %s", h.clientID, serverAddr)
}

func (h *ExampleEventHandler) OnDisconnected(serverAddr string, err error) {
	if err != nil {
		log.Printf("[%s] Disconnected from server: %s, error: %v", h.clientID, serverAddr, err)
	} else {
		log.Printf("[%s] Disconnected from server: %s", h.clientID, serverAddr)
	}
}

func (h *ExampleEventHandler) OnConnectionSwitched(fromAddr, toAddr string) {
	log.Printf("[%s] Connection switched from %s to %s", h.clientID, fromAddr, toAddr)
}

// 消息事件
func (h *ExampleEventHandler) OnMessageReceived(envelope *message.MessageEnvelope) {
	globalID, err := message.FromBytes(envelope.MessageId)
	if err != nil {
		log.Printf("[%s] Received message with invalid ID: %v", h.clientID, err)
		return
	}

	switch msg := envelope.Message.(type) {
	case *message.MessageEnvelope_ChatMessage:
		log.Printf("[%s] Received chat message from user %d: %s",
			h.clientID, globalID.ExtractUserID(), msg.ChatMessage.Content)
	case *message.MessageEnvelope_JoinRoomAck:
		if msg.JoinRoomAck.Status == message.JoinRoomStatus_JOIN_ROOM_SUCCESS {
			log.Printf("[%s] Successfully joined room %d", h.clientID, msg.JoinRoomAck.RoomId)
		} else {
			log.Printf("[%s] Failed to join room: %s", h.clientID, msg.JoinRoomAck.Reason)
		}
	case *message.MessageEnvelope_CreateRoomAck:
		if msg.CreateRoomAck.Status == message.CreateRoomStatus_CREATE_ROOM_SUCCESS {
			log.Printf("[%s] Successfully created room %d", h.clientID, msg.CreateRoomAck.RoomId)
		} else {
			log.Printf("[%s] Failed to create room: %s", h.clientID, msg.CreateRoomAck.Reason)
		}
	default:
		log.Printf("[%s] Received message of type: %T", h.clientID, envelope.Message)
	}
}

func (h *ExampleEventHandler) OnMessageSent(envelope *message.MessageEnvelope) {
	globalID, _ := message.FromBytes(envelope.MessageId)
	log.Printf("[%s] Message sent to room %d", h.clientID, globalID.ExtractRoomID())
}

func (h *ExampleEventHandler) OnMessageDelivered(envelope *message.MessageEnvelope) {
	globalID, _ := message.FromBytes(envelope.MessageId)
	log.Printf("[%s] Message delivered: %x", h.clientID, globalID)
}

func (h *ExampleEventHandler) OnMessageFailed(envelope *message.MessageEnvelope, err error) {
	globalID, _ := message.FromBytes(envelope.MessageId)
	log.Printf("[%s] Message failed: %x, error: %v", h.clientID, globalID, err)
}

// 房间事件
func (h *ExampleEventHandler) OnRoomJoined(roomInfo *message.RoomInfo) {
	log.Printf("[%s] Joined room: %s (ID: %d, Members: %d/%d)",
		h.clientID, roomInfo.Name, roomInfo.RoomId, roomInfo.CurrentMembers, roomInfo.MaxMembers)
}

func (h *ExampleEventHandler) OnRoomLeft(roomID uint64) {
	log.Printf("[%s] Left room: %d", h.clientID, roomID)
}

func (h *ExampleEventHandler) OnRoomMemberJoined(userID uint64) {
	log.Printf("[%s] User %d joined the room", h.clientID, userID)
}

func (h *ExampleEventHandler) OnRoomMemberLeft(userID uint64) {
	log.Printf("[%s] User %d left the room", h.clientID, userID)
}

// 错误事件
func (h *ExampleEventHandler) OnError(err error) {
	log.Printf("[%s] Error: %v", h.clientID, err)
}

// CreateExampleClient 创建示例客户端
func CreateExampleClient(userID uint32, loginID uint8, serverAddrs []string) *LiveChatClient {
	config := &ClientConfig{
		ServerAddrs:       serverAddrs,
		ConnectTimeout:    10 * time.Second,
		ReconnectInterval: 5 * time.Second,
		HeartbeatInterval: 30 * time.Second,
		MessageTimeout:    10 * time.Second,
		MaxRetries:        3,
		BufferSize:        1000,
		FetchTimeout:      5 * time.Second,
		FetchBatchSize:    50,
		FetchMaxRetries:   3,
	}

	handler := NewExampleEventHandler(fmt.Sprintf("User_%d", userID))

	return NewLiveChatClient(userID, loginID, config, handler)
}

// RunExampleScenario 运行示例场景
func RunExampleScenario() {
	log.Println("=== LiveChatroom Client Example ===")

	// 创建多个客户端
	serverAddrs := []string{"ws://localhost:8080", "ws://localhost:8082"}

	client1 := CreateExampleClient(1001, 1, serverAddrs)
	client2 := CreateExampleClient(1002, 1, serverAddrs)

	// 启动客户端
	log.Println("Starting clients...")

	if err := client1.Start(); err != nil {
		log.Printf("Failed to start client1: %v", err)
		return
	}
	defer client1.Stop()

	if err := client2.Start(); err != nil {
		log.Printf("Failed to start client2: %v", err)
		return
	}
	defer client2.Stop()

	// 等待连接建立
	time.Sleep(2 * time.Second)

	// 客户端1创建房间
	log.Println("Client1 creating room...")
	if err := client1.CreateRoom(12345, "Test Room", 10, ""); err != nil {
		log.Printf("Failed to create room: %v", err)
	}

	time.Sleep(1 * time.Second)

	// 客户端2加入房间
	log.Println("Client2 joining room...")
	if err := client2.JoinRoom(12345, ""); err != nil {
		log.Printf("Failed to join room: %v", err)
	}

	time.Sleep(1 * time.Second)

	// 发送聊天消息
	log.Println("Sending chat messages...")

	if err := client1.SendChatMessage("Hello from client1!", message.MessageType_MESSAGE_TYPE_TEXT, 0); err != nil {
		log.Printf("Failed to send message from client1: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	if err := client2.SendChatMessage("Hello from client2!", message.MessageType_MESSAGE_TYPE_TEXT, 0); err != nil {
		log.Printf("Failed to send message from client2: %v", err)
	}

	// 等待消息处理
	time.Sleep(2 * time.Second)

	// 拉取房间消息
	log.Println("Fetching room messages...")
	if err := client1.FetchRoomMessages(0, 10, 0); err != nil {
		log.Printf("Failed to fetch room messages: %v", err)
	}

	// 等待更多处理
	time.Sleep(2 * time.Second)

	// 显示统计信息
	stats1 := client1.GetStats()
	stats2 := client2.GetStats()

	log.Printf("Client1 stats: Sent=%d, Received=%d, Retries=%d, Fetches=%d, Switches=%d",
		stats1.TotalMessagesSent, stats1.TotalMessagesReceived,
		stats1.TotalRetries, stats1.TotalFetches, stats1.ConnectionSwitches)

	log.Printf("Client2 stats: Sent=%d, Received=%d, Retries=%d, Fetches=%d, Switches=%d",
		stats2.TotalMessagesSent, stats2.TotalMessagesReceived,
		stats2.TotalRetries, stats2.TotalFetches, stats2.ConnectionSwitches)

	log.Println("=== Example completed ===")
}
