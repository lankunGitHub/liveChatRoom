-- liveChatRoom 数据库初始化脚本
-- MySQL 8.0+，字符集 utf8mb4
-- 服务启动时也会自动建表（见 msgcenter/server.go initDatabaseTables），
-- 本文件用于提前部署或DBA审查

CREATE DATABASE IF NOT EXISTS livechat DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE livechat;

-- 消息表
-- message_id 唯一索引：Kafka重复消费/重放时通过 INSERT IGNORE 幂等去重
-- (room_id, created_at) 联合索引：房间历史消息按时间倒序查询
CREATE TABLE IF NOT EXISTS messages (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    message_id BINARY(16) NOT NULL UNIQUE COMMENT '128位全局消息ID（时间戳48位+用户32位+房间32位+登录4位+序号6位+自定义6位）',
    user_id INT UNSIGNED NOT NULL COMMENT '发送者ID',
    room_id BIGINT UNSIGNED NOT NULL COMMENT '房间ID',
    login_id TINYINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '设备登录ID',
    message_type VARCHAR(50) NOT NULL COMMENT '消息类型: chat/create_room/join_room/...',
    content TEXT COMMENT '消息内容',
    data BLOB COMMENT '序列化后的完整消息',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_message_id (message_id),
    INDEX idx_room_time (room_id, created_at),
    INDEX idx_user_room_time (user_id, room_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='聊天消息表，按message_id幂等';

-- 用户表
CREATE TABLE IF NOT EXISTS users (
    id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(100) NOT NULL UNIQUE,
    email VARCHAR(255),
    avatar_url VARCHAR(500),
    status TINYINT NOT NULL DEFAULT 1 COMMENT '1=正常 0=禁用',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 房间表
CREATE TABLE IF NOT EXISTS rooms (
    id BIGINT UNSIGNED PRIMARY KEY COMMENT '房间ID由路由服务器全局分配',
    name VARCHAR(255) NOT NULL,
    creator_id INT UNSIGNED NOT NULL,
    max_members INT UNSIGNED NOT NULL DEFAULT 100,
    password VARCHAR(255) COMMENT '房间密码，空表示无密码',
    status TINYINT NOT NULL DEFAULT 0 COMMENT '0=开放 1=关闭',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_creator (creator_id),
    INDEX idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 房间成员表
CREATE TABLE IF NOT EXISTS room_members (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    room_id BIGINT UNSIGNED NOT NULL,
    user_id INT UNSIGNED NOT NULL,
    role TINYINT NOT NULL DEFAULT 0 COMMENT '0=成员 1=房主',
    joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    left_at TIMESTAMP NULL COMMENT '离开时间，NULL表示仍在房间',
    UNIQUE KEY uk_room_user (room_id, user_id),
    INDEX idx_user (user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 消息量级大时的演进方向：
-- 1. 按 room_id 取模分表（如 messages_0 ~ messages_127），路由服务器保证房间映射稳定
-- 2. 历史消息归档：messages_archive 按月分区，冷数据定时迁移
-- 3. 已过保留期（默认7天）的消息由 msgcenter 定时清理（CleanupExpiredMessages）
