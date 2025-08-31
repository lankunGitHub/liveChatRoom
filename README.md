# liveChatRoom

基于 Go 的分布式实时聊天系统，包含一个自研的高性能网络框架和完整的三层服务架构。

## 架构

```
客户端 (client) ──► 连接服务器 (connection) ──► 路由服务器 (router)
                        │                           │
                        └────── Kafka ──────► 消息中心 (msgcenter)
                                                    │
                                                MySQL 落库
```

- **连接服务器 (connection)**：负责客户端连接管理、房间管理和消息处理，多节点部署
- **路由服务器 (router)**：节点负载均衡和跨节点消息路由，节点状态保存在 Redis
- **消息中心 (msgcenter)**：Kafka 消费消息，MySQL 批量落库，支持历史消息查询
- **客户端 SDK (client)**：支持多节点连接切换、消息重传确认、排序去重

## 目录结构

```
liveChatRoom/
├── client/        # 客户端 SDK
├── connection/    # 连接服务器
├── router/        # 路由服务器
├── msgcenter/     # 消息中心
├── message/       # 消息协议定义和编解码 (protobuf)
├── config/        # 配置加载
└── util/
    ├── net/       # 自研网络框架（epoll + 零拷贝 + 事件驱动）
    │   ├── base/      # buffer/socket/epoll/io/timer 基础组件
    │   ├── protocol/  # HTTP/WebSocket 协议解析
    │   ├── net/       # reactor/engine/listener 网络引擎
    │   └── api/       # 对外 API 封装
    └── pool/      # 通用 goroutine 池
```

## 核心特性

- **消息可靠性**：发送 → 等待 ACK → 超时重传 → 切换节点 fetch 确认
- **多节点容灾**：客户端维护多个连接节点，断线自动切换
- **消息排序去重**：基于全局消息 ID（雪花算法）的排序和去重
- **协议无关**：网络框架支持协议注册和自动检测（HTTP / WebSocket）

## 构建

```bash
go build ./...
go test ./...
```

## 配置

配置文件 `config.yaml`，包含连接服务器、路由服务器、消息中心、客户端的完整配置，以及 Redis / MySQL / Kafka 连接信息。

## 技术栈

- Go 1.24
- Redis：节点状态、在线状态
- MySQL：消息持久化
- Kafka：消息总线
- protobuf：消息协议
