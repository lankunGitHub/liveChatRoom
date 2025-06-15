# NetFramework - 高性能Go网络框架

一个基于零拷贝技术和事件驱动架构的高性能Go网络框架，支持HTTP和WebSocket协议。

## 🏗️ 架构设计

框架采用清晰的4层架构设计：

### 📦 基础层 (Base Layer)
- **`base/buffer/`** - 统一的缓冲区接口和高性能环形缓冲区实现
- **`base/socket/`** - Socket系统调用封装，支持TCP/UDP
- **`base/epoll/`** - Linux epoll事件循环封装
- **`base/io/`** - 零拷贝I/O操作（readv/writev/sendfile）
- **`base/timer/`** - 高精度定时器实现
- **`base/pool/`** - 通用对象池，减少GC压力

### 🔌 协议层 (Protocol Layer)
- **`protocol/`** - 统一协议接口和智能协议检测
- **`protocol/http/`** - RFC标准HTTP协议解析和构建
- **`protocol/websocket/`** - RFC 6455 WebSocket协议解析和构建
- **协议注册表** - 支持动态协议注册和扩展

### 🌐 网络层 (Network Layer)
- **`net/connection/`** - 高性能连接抽象和管理
- **`net/reactor/`** - 事件驱动反应器模式实现
- **`net/listener/`** - 网络监听器和连接接受
- **`net/engine/`** - 网络引擎，整合所有网络组件

### 🚀 应用层 (API Layer)
- **`api/server.go`** - 简洁的HTTP/WebSocket服务器API
- **`api/client.go`** - HTTP和WebSocket客户端实现
- **`api/example.go`** - 完整的使用示例
- **`api/demo.go`** - 性能演示和聊天室示例

## ✨ 核心特性

### 🔥 高性能
- **零拷贝I/O** - 基于向量I/O和sendfile的高效数据传输
- **事件驱动** - 基于Linux epoll的异步事件处理
- **多反应器** - 支持多个反应器的负载均衡
- **连接池化** - 对象复用减少内存分配和GC压力
- **流式解析** - 协议数据的增量解析

### 🧩 高扩展性
- **协议无关** - 支持任意协议的插件式扩展
- **组件化设计** - 每个模块可独立使用和配置
- **中间件系统** - 丰富的中间件支持
- **接口驱动** - 基于接口的松耦合设计

### 🛡️ 高可靠性
- **优雅关闭** - 完整的资源清理和同步机制
- **错误恢复** - 全面的错误处理和自动恢复
- **连接管理** - 自动的连接清理和超时处理
- **状态跟踪** - 线程安全的状态管理

## 🚀 快速开始

### HTTP服务器
```go
package main

import "liveChatroom/util/net/api"

func main() {
    // 创建服务器
    server := api.Default()
    
    // 定义路由
    server.GET("/hello", func(c *api.Context) {
        c.JSON(200, map[string]interface{}{
            "message": "Hello, NetFramework!",
        })
    })
    
    // 启动服务器
    server.Run(":8080")
}
```

### HTTP客户端
```go
// 快速请求
resp, err := api.Get("http://localhost:8080/hello")
if err == nil {
    fmt.Println(string(resp.Body))
}

// 或者使用客户端实例
client := api.NewHTTPClient()
defer client.Close()

resp, err = client.GET("http://localhost:8080/hello")
```

### WebSocket服务器
```go
server := api.New()

server.WebSocket("/ws", func(c *api.Context) {
    fmt.Println("WebSocket connection established")
    
    c.Connection.OnMessage(func(conn *connection.Connection, msg protocol.Message) {
        fmt.Printf("Received: %s\n", string(msg.GetPayload()))
        // Echo message back
        conn.WriteMessage(msg)
    })
})

server.Run(":8080")
```

### WebSocket客户端
```go
client, err := api.ConnectWebSocket("ws://localhost:8080/ws", func(data []byte) {
    fmt.Printf("Received: %s\n", string(data))
})

if err == nil {
    client.SendText("Hello WebSocket!")
    defer client.Close()
}
```

## 🛠️ 高级功能

### 中间件系统
```go
server := api.Default() // 包含Logger和Recovery中间件

// 自定义中间件
server.Use(func(c *api.Context) {
    start := time.Now()
    c.Next()
    fmt.Printf("Request took: %v\n", time.Since(start))
})

// 路由组中间件
apiGroup := server.Group("/api", api.AuthMiddleware())
```

### 协议升级
```go
server.GET("/upgrade", func(c *api.Context) {
    // 处理HTTP请求
    if c.Request.GetHeaders()["upgrade"] == "websocket" {
        // 自动处理WebSocket升级
    }
})
```

### 性能配置
```go
server := api.NewConfig().
    Address(":8080").
    MaxConnections(50000).
    Concurrency(8, 8).  // 8个反应器，每个8个工作协程
    Timeout(30*time.Second, 30*time.Second, 5*time.Minute).
    Build()
```

## 📊 性能指标

- **连接数**: 支持数万并发连接
- **吞吐量**: 基于epoll和零拷贝的高吞吐量
- **内存**: 对象池化减少内存分配
- **延迟**: 事件驱动架构降低延迟

## 🔧 组件独立使用

每个层的组件都可以独立使用：

```go
// 只使用基础层
import "liveChatroom/util/net/base/buffer"
buf := buffer.Get(8192)

// 只使用协议层
import "liveChatroom/util/net/protocol/http"
parser := http.NewHTTPParser()

// 只使用网络层
import "liveChatroom/util/net/net/reactor"
reactor, _ := reactor.NewReactor(4)
```

## 🧪 示例和演示

运行完整的演示：

```go
// 基础演示
api.RunDemo()

// 性能基准测试
api.BenchmarkDemo()

// 聊天室演示
api.ChatRoomDemo()

// 快速开始
api.QuickStart()
```

## 📋 支持的协议

- ✅ **HTTP/1.1** - 完整的RFC 7230-7237支持
- ✅ **WebSocket** - RFC 6455标准实现
- 🔄 **HTTP/2** - 计划支持
- 🔄 **自定义协议** - 通过协议注册表扩展

## 🏆 设计优势

1. **零依赖** - 只使用Go标准库
2. **零拷贝** - 最小化内存拷贝操作
3. **零配置** - 开箱即用的默认配置
4. **高性能** - 接近操作系统原生性能
5. **易扩展** - 清晰的层次结构和接口设计
6. **易使用** - 简洁的API和丰富的示例

## 🤝 贡献指南

欢迎贡献代码、报告问题或提出建议！

## 📄 许可证

MIT License

---

**NetFramework** - 让高性能网络编程变得简单！ 🚀
