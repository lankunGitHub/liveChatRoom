package api

import (
	"fmt"
	"liveChatroom/util/net/net/connection"
	"liveChatroom/util/net/protocol"
	"log"
	"time"
)

// ExampleServerHandler 示例服务器事件处理器
type ExampleServerHandler struct{}

func (h *ExampleServerHandler) OnConnectionAccepted(conn *connection.Connection) {
	log.Printf("New connection accepted: %s", conn.RemoteAddr())
}

func (h *ExampleServerHandler) OnConnectionClosed(conn *connection.Connection, err error) {
	if err != nil {
		log.Printf("Connection closed with error: %s - %v", conn.RemoteAddr(), err)
	} else {
		log.Printf("Connection closed: %s", conn.RemoteAddr())
	}
}

func (h *ExampleServerHandler) OnMessageReceived(conn *connection.Connection, msg protocol.Message) error {
	log.Printf("Message received from %s: %d bytes", conn.RemoteAddr(), len(msg.GetPayload()))

	// Echo back the message
	conn.WriteMessage(msg)

	return nil
}

func (h *ExampleServerHandler) OnError(err error) {
	log.Printf("Server error: %v", err)
}

// ExampleClientHandler 示例客户端事件处理器
type ExampleClientHandler struct{}

func (h *ExampleClientHandler) OnConnected(client *Client) {
	log.Printf("Client connected successfully")
}

func (h *ExampleClientHandler) OnDisconnected(client *Client, err error) {
	if err != nil {
		log.Printf("Client disconnected with error: %v", err)
	} else {
		log.Printf("Client disconnected")
	}
}

func (h *ExampleClientHandler) OnReconnected(client *Client, attempt int) {
	log.Printf("Client reconnected after %d attempts", attempt)
}

func (h *ExampleClientHandler) OnMessageReceived(client *Client, msg protocol.Message) error {
	log.Printf("Client received message: %d bytes", len(msg.GetPayload()))
	return nil
}

func (h *ExampleClientHandler) OnHeartbeatSent(client *Client) {
	log.Printf("Heartbeat sent")
}

func (h *ExampleClientHandler) OnHeartbeatReceived(client *Client) {
	log.Printf("Heartbeat received")
}

func (h *ExampleClientHandler) OnError(client *Client, err error) {
	log.Printf("Client error: %v", err)
}

// RunEchoServer 运行Echo服务器示例
func RunEchoServer() {
	// 创建服务器配置
	config := &ServerConfig{
		Address:         ":8080",
		MaxConnections:  1000,
		ReadTimeout:     30 * time.Second,
		WriteTimeout:    30 * time.Second,
		IdleTimeout:     5 * time.Minute,
		ReactorCount:    2,
		WorkerCount:     4,
		ReadBufferSize:  8192,
		WriteBufferSize: 8192,
	}

	// 创建事件处理器
	handler := &ExampleServerHandler{}

	// 创建服务器
	server, err := NewServer(config, handler)
	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}

	// 启动服务器
	log.Printf("Starting echo server on %s", config.Address)
	if err := server.Start(); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}

	// 等待服务器运行
	for {
		if !server.IsRunning() {
			break
		}

		// 打印统计信息
		stats := server.GetStats()
		log.Printf("Server stats - Active: %d, Total: %d, Messages: %d, Uptime: %v",
			stats.ActiveConnections,
			stats.TotalConnections,
			stats.MessagesReceived,
			server.GetUptime(),
		)

		time.Sleep(10 * time.Second)
	}
}

// RunEchoClient 运行Echo客户端示例
func RunEchoClient() {
	// 创建客户端配置
	config := &ClientConfig{
		ConnectTimeout:    10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		ReadBufferSize:    8192,
		WriteBufferSize:   8192,
		EnableReconnect:   true,
		ReconnectInterval: 5 * time.Second,
		MaxReconnectTries: 3,
		EnableHeartbeat:   true,
		HeartbeatInterval: 30 * time.Second,
		HeartbeatTimeout:  10 * time.Second,
	}

	// 创建事件处理器
	handler := &ExampleClientHandler{}

	// 创建客户端
	client, err := NewClient(config, handler)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	// 连接到服务器
	log.Printf("Connecting to server...")
	if err := client.Connect("localhost:8080"); err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}

	// 发送测试消息
	for i := 0; i < 10; i++ {
		message := fmt.Sprintf("Hello, this is message #%d", i+1)
		if err := client.Send([]byte(message)); err != nil {
			log.Printf("Failed to send message: %v", err)
			break
		}

		log.Printf("Sent: %s", message)
		time.Sleep(2 * time.Second)
	}

	// 打印统计信息
	stats := client.GetStats()
	log.Printf("Client stats - Sent: %d, Received: %d, Uptime: %v",
		stats.MessagesSent,
		stats.MessagesReceived,
		client.GetUptime(),
	)

	// 断开连接
	if err := client.Disconnect(); err != nil {
		log.Printf("Failed to disconnect: %v", err)
	}
}

// RunPerformanceTest 运行性能测试
func RunPerformanceTest() {
	log.Printf("Starting performance test...")

	// 高性能配置
	serverConfig := &ServerConfig{
		Address:         ":8081",
		MaxConnections:  10000,
		ReadTimeout:     10 * time.Second,
		WriteTimeout:    10 * time.Second,
		IdleTimeout:     1 * time.Minute,
		ReactorCount:    4,
		WorkerCount:     8,
		ReadBufferSize:  16384,
		WriteBufferSize: 16384,
	}

	handler := &ExampleServerHandler{}
	server, err := NewServer(serverConfig, handler)
	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}

	// 启动服务器
	go func() {
		if err := server.Start(); err != nil {
			log.Fatalf("Failed to start server: %v", err)
		}
	}()

	// 等待服务器启动
	time.Sleep(1 * time.Second)

	// 创建多个客户端连接
	const numClients = 100
	const messagesPerClient = 10

	clientHandler := &ExampleClientHandler{}
	clients := make([]*Client, numClients)

	// 创建并连接客户端
	for i := 0; i < numClients; i++ {
		client, err := NewClient(DefaultClientConfig(), clientHandler)
		if err != nil {
			log.Printf("Failed to create client %d: %v", i, err)
			continue
		}

		if err := client.Connect("localhost:8081"); err != nil {
			log.Printf("Failed to connect client %d: %v", i, err)
			continue
		}

		clients[i] = client
	}

	log.Printf("Created %d client connections", numClients)

	// 并发发送消息
	startTime := time.Now()

	for i := 0; i < numClients; i++ {
		go func(clientIndex int) {
			client := clients[clientIndex]
			if client == nil {
				return
			}

			for j := 0; j < messagesPerClient; j++ {
				message := fmt.Sprintf("Client %d - Message %d", clientIndex, j)
				if err := client.Send([]byte(message)); err != nil {
					log.Printf("Client %d send error: %v", clientIndex, err)
					break
				}
				time.Sleep(10 * time.Millisecond) // 小间隔避免过载
			}
		}(i)
	}

	// 等待消息发送完成
	time.Sleep(5 * time.Second)

	elapsed := time.Since(startTime)
	totalMessages := numClients * messagesPerClient
	messagesPerSecond := float64(totalMessages) / elapsed.Seconds()

	// 打印性能统计
	serverStats := server.GetStats()
	log.Printf("Performance Test Results:")
	log.Printf("  Duration: %v", elapsed)
	log.Printf("  Total Messages: %d", totalMessages)
	log.Printf("  Messages/Second: %.2f", messagesPerSecond)
	log.Printf("  Server Active Connections: %d", serverStats.ActiveConnections)
	log.Printf("  Server Total Messages: %d", serverStats.MessagesReceived)

	// 清理客户端
	for _, client := range clients {
		if client != nil {
			client.Disconnect()
		}
	}

	// 停止服务器
	server.Stop()

	log.Printf("Performance test completed")
}

// ShowUsage 显示使用方法
func ShowUsage() {
	fmt.Print(`
NetFramework API 使用示例:

1. 创建Echo服务器:
   api.RunEchoServer()

2. 创建Echo客户端:
   api.RunEchoClient()

3. 运行性能测试:
   api.RunPerformanceTest()

基本用法:

服务器端:
  config := api.DefaultServerConfig()
  handler := &MyServerHandler{}
  server, _ := api.NewServer(config, handler)
  server.Start()

客户端:
  config := api.DefaultClientConfig()
  handler := &MyClientHandler{}
  client, _ := api.NewClient(config, handler)
  client.Connect("localhost:8080")
  client.Send([]byte("Hello"))

特性:
  - 零拷贝I/O
  - 事件驱动架构
  - 高并发支持
  - 自动重连
  - 心跳检测
  - 连接池化
  - 协议无关设计
`)
}
