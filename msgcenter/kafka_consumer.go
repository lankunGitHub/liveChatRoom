package msgcenter

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"
)

// KafkaConsumer Kafka消费者 - 消费消息队列中的消息并批量落库
// 配合INSERT IGNORE和message_id唯一索引，重复消费/重放是幂等的
type KafkaConsumer struct {
	reader *kafka.Reader
	db     *DatabaseManager
	config *MessageCenterConfig

	// 批量落库缓冲
	batch      []*PersistMessage
	batchMutex sync.Mutex

	// 统计信息
	messagesConsumed  int64
	messagesPersisted int64
	messagesFailed    int64

	// 控制
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewKafkaConsumer 创建Kafka消费者
func NewKafkaConsumer(db *DatabaseManager, config *MessageCenterConfig) *KafkaConsumer {
	return &KafkaConsumer{
		db:     db,
		config: config,
		batch:  make([]*PersistMessage, 0, config.BatchInsertSize),
	}
}

// Start 启动Kafka消费者
func (kc *KafkaConsumer) Start(ctx context.Context) {
	kc.ctx, kc.cancel = context.WithCancel(ctx)

	// 初始化Kafka读取器
	kc.reader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:     kc.config.Kafka.Brokers,
		Topic:       kc.config.Kafka.Topic,
		GroupID:     kc.config.Kafka.GroupID,
		MinBytes:    1,
		MaxBytes:    10e6, // 10MB
		MaxWait:     time.Second,
		StartOffset: kafka.LastOffset, // 只消费新消息，历史消息已通过直写路径落库
	})

	// 启动消费协程
	kc.wg.Add(1)
	go kc.consumeLoop()

	// 启动批量落库协程
	kc.wg.Add(1)
	go kc.batchFlushLoop()

	log.Printf("KafkaConsumer started with brokers: %v, topic: %s, group: %s",
		kc.config.Kafka.Brokers, kc.config.Kafka.Topic, kc.config.Kafka.GroupID)
}

// Stop 停止Kafka消费者
func (kc *KafkaConsumer) Stop() {
	if kc.cancel != nil {
		kc.cancel()
	}
	kc.wg.Wait()

	// 落库剩余批次
	kc.flushBatch()

	if kc.reader != nil {
		kc.reader.Close()
	}

	log.Printf("KafkaConsumer stopped")
}

// consumeLoop 消费循环
func (kc *KafkaConsumer) consumeLoop() {
	defer kc.wg.Done()

	for {
		select {
		case <-kc.ctx.Done():
			return
		default:
		}

		ctx, cancel := context.WithTimeout(kc.ctx, 5*time.Second)
		m, err := kc.reader.FetchMessage(ctx)
		cancel()

		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				continue // 正常超时或退出，继续循环
			}
			log.Printf("Failed to fetch message from kafka: %v", err)
			atomic.AddInt64(&kc.messagesFailed, 1)
			time.Sleep(time.Second) // 出错后退避，避免忙轮询
			continue
		}

		// 解析消息
		persistMsg := kc.parseKafkaMessage(m.Value)
		if persistMsg != nil {
			kc.addToBatch(persistMsg)
			atomic.AddInt64(&kc.messagesConsumed, 1)
		}

		// 提交offset（落库失败的消息依赖重放恢复，message_id唯一索引保证幂等）
		commitCtx, commitCancel := context.WithTimeout(kc.ctx, 5*time.Second)
		if err := kc.reader.CommitMessages(commitCtx, m); err != nil {
			log.Printf("Failed to commit kafka offset: %v", err)
		}
		commitCancel()
	}
}

// parseKafkaMessage 解析Kafka消息为待持久化消息
func (kc *KafkaConsumer) parseKafkaMessage(data []byte) *PersistMessage {
	var kafkaMsg KafkaMessage
	if err := json.Unmarshal(data, &kafkaMsg); err != nil {
		log.Printf("Failed to unmarshal kafka message: %v", err)
		atomic.AddInt64(&kc.messagesFailed, 1)
		return nil
	}

	// 解析消息ID
	messageID, err := hex.DecodeString(kafkaMsg.MessageID)
	if err != nil {
		log.Printf("Failed to decode message id: %v", err)
		atomic.AddInt64(&kc.messagesFailed, 1)
		return nil
	}

	info := &MessageInfo{
		MessageID:   messageID,
		UserID:      kafkaMsg.UserID,
		RoomID:      kafkaMsg.RoomID,
		LoginID:     kafkaMsg.LoginID,
		Timestamp:   kafkaMsg.Timestamp,
		MessageType: kafkaMsg.MessageType,
		Content:     kafkaMsg.Content,
		Data:        kafkaMsg.Data,
	}

	return &PersistMessage{
		receivedTime: time.Now(),
		messageInfo:  info,
	}
}

// addToBatch 添加到批量落库缓冲
func (kc *KafkaConsumer) addToBatch(persistMsg *PersistMessage) {
	kc.batchMutex.Lock()
	defer kc.batchMutex.Unlock()

	kc.batch = append(kc.batch, persistMsg)

	// 达到批量大小立即落库
	if len(kc.batch) >= kc.config.BatchInsertSize {
		batch := kc.batch
		kc.batch = make([]*PersistMessage, 0, kc.config.BatchInsertSize)
		go kc.persistBatch(batch)
	}
}

// batchFlushLoop 批量落库循环
func (kc *KafkaConsumer) batchFlushLoop() {
	defer kc.wg.Done()

	flushInterval := 5 * time.Second
	if kc.config.Kafka.BatchTimeout > 0 {
		flushInterval = kc.config.Kafka.BatchTimeout
	}

	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-kc.ctx.Done():
			return
		case <-ticker.C:
			kc.flushBatch()
		}
	}
}

// flushBatch 落库当前批次
func (kc *KafkaConsumer) flushBatch() {
	kc.batchMutex.Lock()
	if len(kc.batch) == 0 {
		kc.batchMutex.Unlock()
		return
	}
	batch := kc.batch
	kc.batch = make([]*PersistMessage, 0, kc.config.BatchInsertSize)
	kc.batchMutex.Unlock()

	kc.persistBatch(batch)
}

// persistBatch 执行批量落库
func (kc *KafkaConsumer) persistBatch(batch []*PersistMessage) {
	if err := kc.db.BatchInsertMessages(batch); err != nil {
		log.Printf("Failed to persist kafka message batch: %v", err)
		atomic.AddInt64(&kc.messagesFailed, int64(len(batch)))
		return
	}

	atomic.AddInt64(&kc.messagesPersisted, int64(len(batch)))
}

// GetStats 获取消费者统计信息
func (kc *KafkaConsumer) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"messages_consumed":  atomic.LoadInt64(&kc.messagesConsumed),
		"messages_persisted": atomic.LoadInt64(&kc.messagesPersisted),
		"messages_failed":    atomic.LoadInt64(&kc.messagesFailed),
		"pending_batch":      len(kc.batch),
	}
}
