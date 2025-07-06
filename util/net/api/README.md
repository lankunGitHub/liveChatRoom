# NetFramework API Layer

高性能Go网络库的应用层API，提供简洁的服务器和客户端接口。

## 📋 设计原则

- **底层独立**: 不依赖任何业务逻辑，纯粹的网络库
- **零拷贝**: 基于底层零拷贝I/O实现
- **事件驱动**: 异步事件处理架构
- **协议无关**: 支持任意协议扩展
- **高性能**: 多反应器 + 连接池化

## 🏗️ 架构分层

```
├── api/           (应用层) ← 当前层
│   ├── server.go     - 网络服务器
│   ├── client.go     - 网络客户端  
│   └── example.go    - 使用示例
├── net/           (网络层)
│   ├── engine/       - 网络引擎
│   ├── connection/   - 连接管理
│   ├── reactor/      - 事件反应器
│   └── listener/     - 网络监听
├── protocol/      (协议层)
│   ├── http/         - HTTP协议
│   └── websocket/    - WebSocket协议
└── base/          (基础层)
    ├── buffer/       - 缓冲区管理
    ├── socket/       - Socket封装
    ├── epoll/        - 事件循环
    └── io/           - 零拷贝I/O
```

## 🚀 快速开始

### 服务器端

```go
package main

import (
    "liveChatroom/util/net/api"
    "log"
)

// 实现事件处理器
type MyHandler struct{}

func (h *MyHandler) OnConnectionAccepted(conn *connection.Connection) {
    log.Printf("New connection: %s", conn.RemoteAddr())
}

func (h *MyHandler) OnMessageReceived(conn *connection.Connection, msg protocol.Message) error {
    // 处理消息
    log.Printf("Received: %d bytes", len(msg.GetPayload()))
    
    // Echo back
    conn.WriteMessage(msg)
    return nil
}

func (h *MyHandler) OnConnectionClosed(conn *connection.Connection, err error) {
    log.Printf("Connection closed: %s", conn.RemoteAddr())
}

func (h *MyHandler) OnError(err error) {
    log.Printf("Error: %v", err)
}

func main() {
    // 创建配置
    config := &api.ServerConfig{
        Address:         ":8080",
        MaxConnections:  10000,
        ReactorCount:    4,
        WorkerCount:     8,
    }
    
    // 创建服务器
    server, err := api.NewServer(config, &MyHandler{})
    if err != nil {
        log.Fatal(err)
    }
    
    // 启动服务器
    log.Println("Starting server...")
    if err := server.Start(); err != nil {
        log.Fatal(err)
    }
    
    // 保持运行
    select {}
}
```

### 客户端

```go
package main

import (
    "liveChatroom/util/net/api"
    "log"
    "time"
)

// 实现客户端事件处理器
type MyClientHandler struct{}

func (h *MyClientHandler) OnConnected(client *api.Client) {
    log.Println("Connected to server")
}

func (h *MyClientHandler) OnMessageReceived(client *api.Client, msg protocol.Message) error {
    log.Printf("Received: %s", string(msg.GetPayload()))
    return nil
}

func (h *MyClientHandler) OnDisconnected(client *api.Client, err error) {
    log.Println("Disconnected from server")
}

func (h *MyClientHandler) OnReconnected(client *api.Client, attempt int) {
    log.Printf("Reconnected after %d attempts", attempt)
}

func (h *MyClientHandler) OnHeartbeatSent(client *api.Client) {}
func (h *MyClientHandler) OnHeartbeatReceived(client *api.Client) {}
func (h *MyClientHandler) OnError(client *api.Client, err error) {
    log.Printf("Client error: %v", err)
}

func main() {
    // 创建客户端
    client, err := api.NewClient(api.DefaultClientConfig(), &MyClientHandler{})
    if err != nil {
        log.Fatal(err)
    }
    
    // 连接服务器
    if err := client.Connect("localhost:8080"); err != nil {
        log.Fatal(err)
    }
    
    // 发送消息
    for i := 0; i < 10; i++ {
        message := fmt.Sprintf("Hello #%d", i)
        client.Send([]byte(message))
        time.Sleep(time.Second)
    }
    
    // 断开连接
    client.Disconnect()
}
```

## ⚙️ 配置选项

### 服务器配置

```go
type ServerConfig struct {
    // 网络配置
    Address        string        // 监听地址，如 ":8080"
    MaxConnections int           // 最大并发连接数
    ReadTimeout    time.Duration // 读取超时
    WriteTimeout   time.Duration // 写入超时
    IdleTimeout    time.Duration // 空闲超时
    
    // 性能配置
    ReactorCount    int // 反应器数量（建议 = CPU核数）
    WorkerCount     int // 每个反应器的工作协程数
    ReadBufferSize  int // 读取缓冲区大小
    WriteBufferSize int // 写入缓冲区大小
}
```

### 客户端配置

```go
type ClientConfig struct {
    // 连接配置
    ConnectTimeout time.Duration // 连接超时
    ReadTimeout    time.Duration // 读取超时
    WriteTimeout   time.Duration // 写入超时
    
    // 缓冲区配置
    ReadBufferSize  int // 读取缓冲区大小
    WriteBufferSize int // 写入缓冲区大小
    
    // 重连配置
    EnableReconnect   bool          // 启用自动重连
    ReconnectInterval time.Duration // 重连间隔
    MaxReconnectTries int           // 最大重连次数
    
    // 心跳配置
    EnableHeartbeat   bool          // 启用心跳检测
    HeartbeatInterval time.Duration // 心跳间隔
    HeartbeatTimeout  time.Duration // 心跳超时
}
```

## 📊 性能特性

- **高并发**: 支持万级并发连接
- **低延迟**: 零拷贝I/O + 事件驱动
- **高吞吐**: 多反应器负载均衡
- **内存优化**: 缓冲区池化复用
- **CPU优化**: 无锁设计 + 原子操作

## 🧪 示例和测试

### 运行Echo服务器

```go
import "liveChatroom/util/net/api"

// 启动Echo服务器
api.RunEchoServer()
```

### 运行Echo客户端

```go
// 启动Echo客户端
api.RunEchoClient()
```

### 性能测试

```go
// 运行性能测试（100个并发客户端）
api.RunPerformanceTest()
```

### 查看使用说明

```go
// 显示详细使用方法
api.ShowUsage()
```

## 📈 监控和统计

### 服务器统计

```go
stats := server.GetStats()
fmt.Printf("Active Connections: %d\n", stats.ActiveConnections)
fmt.Printf("Total Messages: %d\n", stats.MessagesReceived)
fmt.Printf("Uptime: %v\n", server.GetUptime())
```

### 客户端统计

```go
stats := client.GetStats()
fmt.Printf("Messages Sent: %d\n", stats.MessagesSent)
fmt.Printf("Messages Received: %d\n", stats.MessagesReceived)
fmt.Printf("Reconnect Count: %d\n", stats.ReconnectCount)
```

## 🔧 高级用法

### 带过滤器的广播

```go
// 广播给所有WebSocket连接
server.BroadcastWithFilter(data, func(conn *connection.Connection) bool {
    return conn.GetProtocolType() == protocol.ProtocolWebSocket
})
```

### 自定义协议处理

```go
// 在事件处理器中处理特定协议
func (h *MyHandler) OnMessageReceived(conn *connection.Connection, msg protocol.Message) error {
    switch conn.GetProtocolType() {
    case protocol.ProtocolHTTP:
        return h.handleHTTP(conn, msg)
    case protocol.ProtocolWebSocket:
        return h.handleWebSocket(conn, msg)
    default:
        return fmt.Errorf("unsupported protocol")
    }
}
```

### 连接管理

```go
// 获取活跃连接数
count := server.GetActiveConnectionCount()

// 检查服务器状态
if server.IsRunning() {
    log.Println("Server is running")
}
```

## 🤝 与业务层集成

此API层为纯网络库，业务层可以基于此构建：

- **聊天室服务**: 房间管理、消息路由
- **游戏服务器**: 实时通信、状态同步  
- **API网关**: 请求转发、负载均衡
- **消息中间件**: 消息持久化、分发

## 🔗 相关文档

- [基础层文档](../base/README.md)
- [协议层文档](../protocol/README.md)
- [网络层文档](../net/README.md)
- [完整框架文档](../README.md)

## 📄 许可证

MIT License
