package msgcenter

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"
)

// KafkaManager Kafka管理器 - 负责消息队列操作
type KafkaManager struct {
	config *MessageCenterConfig

	// Kafka写入器
	writer *kafka.Writer

	// 批量消息缓冲
	messageBatch []kafka.Message
	batchMutex   sync.Mutex

	// 统计信息
	messagesSent    int64
	messagesDropped int64
	bytesWritten    int64
	batchesSent     int64

	// 控制
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// KafkaMessage Kafka消息格式
type KafkaMessage struct {
	MessageID   string `json:"message_id"`
	UserID      uint32 `json:"user_id"`
	RoomID      uint32 `json:"room_id"`
	LoginID     uint8  `json:"login_id"`
	MessageType string `json:"message_type"`
	Content     string `json:"content"`
	Timestamp   int64  `json:"timestamp"`
	Data        []byte `json:"data,omitempty"`
}

// NewKafkaManager 创建Kafka管理器
func NewKafkaManager(config *MessageCenterConfig) *KafkaManager {
	return &KafkaManager{
		config:       config,
		messageBatch: make([]kafka.Message, 0, config.Kafka.BatchSize),
	}
}

// Start 启动Kafka管理器
func (km *KafkaManager) Start(ctx context.Context) {
	km.ctx, km.cancel = context.WithCancel(ctx)

	// 初始化Kafka写入器
	km.initKafkaWriter()

	// 启动批量发送协程
	km.wg.Add(1)
	go km.batchSendLoop()

	log.Printf("KafkaManager started with brokers: %v", km.config.Kafka.Brokers)
}

// Stop 停止Kafka管理器
func (km *KafkaManager) Stop() {
	if km.cancel != nil {
		km.cancel()
	}
	km.wg.Wait()

	// 发送剩余的批量消息
	km.flushBatch()

	// 关闭Kafka写入器
	if km.writer != nil {
		km.writer.Close()
	}

	log.Printf("KafkaManager stopped")
}

// initKafkaWriter 初始化Kafka写入器
func (km *KafkaManager) initKafkaWriter() {
	km.writer = &kafka.Writer{
		Addr:         kafka.TCP(km.config.Kafka.Brokers...),
		Topic:        km.config.Kafka.Topic,
		Balancer:     &kafka.LeastBytes{},
		BatchSize:    km.config.Kafka.BatchSize,
		BatchTimeout: km.config.Kafka.BatchTimeout,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		Compression:  kafka.Snappy,
		Logger: kafka.LoggerFunc(func(msg string, args ...interface{}) {
			log.Printf("Kafka: "+msg, args...)
		}),
		ErrorLogger: kafka.LoggerFunc(func(msg string, args ...interface{}) {
			log.Printf("Kafka Error: "+msg, args...)
		}),
	}
}

// SendMessage 发送消息到Kafka
func (km *KafkaManager) SendMessage(persistMsg *PersistMessage) error {
	// 创建Kafka消息
	kafkaMsg, err := km.createKafkaMessage(persistMsg)
	if err != nil {
		return fmt.Errorf("failed to create kafka message: %v", err)
	}

	// 添加到批量队列
	km.addToBatch(kafkaMsg)

	return nil
}

// createKafkaMessage 创建Kafka消息
func (km *KafkaManager) createKafkaMessage(persistMsg *PersistMessage) (kafka.Message, error) {
	// 创建Kafka消息结构
	kafkaMessage := &KafkaMessage{
		MessageID:   fmt.Sprintf("%x", persistMsg.messageInfo.MessageID),
		UserID:      persistMsg.messageInfo.UserID,
		RoomID:      persistMsg.messageInfo.RoomID,
		LoginID:     persistMsg.messageInfo.LoginID,
		MessageType: persistMsg.messageInfo.MessageType,
		Content:     persistMsg.messageInfo.Content,
		Timestamp:   persistMsg.messageInfo.Timestamp,
		Data:        persistMsg.messageInfo.Data,
	}

	// 序列化为JSON
	value, err := json.Marshal(kafkaMessage)
	if err != nil {
		return kafka.Message{}, fmt.Errorf("failed to marshal kafka message: %v", err)
	}

	// 创建Kafka消息
	msg := kafka.Message{
		Key:   []byte(kafkaMessage.MessageID), // 使用消息ID作为key
		Value: value,
		Time:  time.Unix(0, kafkaMessage.Timestamp*int64(time.Millisecond)),
		Headers: []kafka.Header{
			{
				Key:   "message_type",
				Value: []byte(kafkaMessage.MessageType),
			},
			{
				Key:   "room_id",
				Value: []byte(fmt.Sprintf("%d", kafkaMessage.RoomID)),
			},
			{
				Key:   "user_id",
				Value: []byte(fmt.Sprintf("%d", kafkaMessage.UserID)),
			},
		},
	}

	return msg, nil
}

// addToBatch 添加到批量队列
func (km *KafkaManager) addToBatch(msg kafka.Message) {
	km.batchMutex.Lock()
	defer km.batchMutex.Unlock()

	km.messageBatch = append(km.messageBatch, msg)

	// 如果批量队列满了，触发发送
	if len(km.messageBatch) >= km.config.Kafka.BatchSize {
		km.triggerBatchSend()
	}
}

// batchSendLoop 批量发送循环
func (km *KafkaManager) batchSendLoop() {
	defer km.wg.Done()

	ticker := time.NewTicker(km.config.Kafka.BatchTimeout)
	defer ticker.Stop()

	for {
		select {
		case <-km.ctx.Done():
			return

		case <-ticker.C:
			// 定时触发批量发送
			km.batchMutex.Lock()
			if len(km.messageBatch) > 0 {
				km.triggerBatchSend()
			}
			km.batchMutex.Unlock()
		}
	}
}

// triggerBatchSend 触发批量发送（需要持有batchMutex）
func (km *KafkaManager) triggerBatchSend() {
	if len(km.messageBatch) == 0 {
		return
	}

	// 复制当前批量队列
	batch := make([]kafka.Message, len(km.messageBatch))
	copy(batch, km.messageBatch)

	// 清空队列
	km.messageBatch = km.messageBatch[:0]

	// 异步发送到Kafka
	go km.sendBatchToKafka(batch)
}

// sendBatchToKafka 发送批量消息到Kafka
func (km *KafkaManager) sendBatchToKafka(batch []kafka.Message) {
	start := time.Now()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := km.writer.WriteMessages(ctx, batch...); err != nil {
		log.Printf("Failed to write batch to Kafka: %v", err)
		atomic.AddInt64(&km.messagesDropped, int64(len(batch)))
	} else {
		atomic.AddInt64(&km.messagesSent, int64(len(batch)))
		atomic.AddInt64(&km.batchesSent, 1)

		// 计算字节数
		var totalBytes int64
		for _, msg := range batch {
			totalBytes += int64(len(msg.Key) + len(msg.Value))
		}
		atomic.AddInt64(&km.bytesWritten, totalBytes)

		log.Printf("Sent batch of %d messages to Kafka in %v", len(batch), time.Since(start))
	}
}

// flushBatch 刷新批量队列
func (km *KafkaManager) flushBatch() {
	km.batchMutex.Lock()
	defer km.batchMutex.Unlock()

	if len(km.messageBatch) > 0 {
		km.sendBatchToKafka(km.messageBatch)
		km.messageBatch = km.messageBatch[:0]
	}
}

// Cleanup 清理操作
func (km *KafkaManager) Cleanup() {
	// 刷新缓冲区
	km.flushBatch()

	log.Printf("Kafka cleanup completed")
}

// GetStats 获取统计信息
func (km *KafkaManager) GetStats() map[string]interface{} {
	km.batchMutex.Lock()
	batchSize := len(km.messageBatch)
	km.batchMutex.Unlock()

	return map[string]interface{}{
		"messages_sent":      atomic.LoadInt64(&km.messagesSent),
		"messages_dropped":   atomic.LoadInt64(&km.messagesDropped),
		"bytes_written":      atomic.LoadInt64(&km.bytesWritten),
		"batches_sent":       atomic.LoadInt64(&km.batchesSent),
		"pending_batch_size": batchSize,
		"brokers":            km.config.Kafka.Brokers,
		"topic":              km.config.Kafka.Topic,
		"batch_size":         km.config.Kafka.BatchSize,
		"batch_timeout":      km.config.Kafka.BatchTimeout.String(),
	}
}

// CreateTopic 创建Kafka主题（如果不存在）
func (km *KafkaManager) CreateTopic() error {
	conn, err := kafka.Dial("tcp", km.config.Kafka.Brokers[0])
	if err != nil {
		return fmt.Errorf("failed to connect to kafka: %v", err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("failed to get controller: %v", err)
	}

	controllerConn, err := kafka.Dial("tcp", controller.Host+":"+fmt.Sprint(controller.Port))
	if err != nil {
		return fmt.Errorf("failed to connect to controller: %v", err)
	}
	defer controllerConn.Close()

	topicConfigs := []kafka.TopicConfig{
		{
			Topic:             km.config.Kafka.Topic,
			NumPartitions:     3,
			ReplicationFactor: 1,
		},
	}

	err = controllerConn.CreateTopics(topicConfigs...)
	if err != nil {
		return fmt.Errorf("failed to create topic: %v", err)
	}

	log.Printf("Kafka topic '%s' created or already exists", km.config.Kafka.Topic)
	return nil
}

// TestConnection 测试Kafka连接
func (km *KafkaManager) TestConnection() error {
	conn, err := kafka.Dial("tcp", km.config.Kafka.Brokers[0])
	if err != nil {
		return fmt.Errorf("failed to connect to kafka: %v", err)
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions()
	if err != nil {
		return fmt.Errorf("failed to read partitions: %v", err)
	}

	log.Printf("Kafka connection test successful, found %d partitions", len(partitions))
	return nil
}
