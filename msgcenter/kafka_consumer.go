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
// 投递语义为 at-least-once：先落库成功、后提交offset，
// 落库失败的消息不提交，进程重启后由Kafka重放恢复；
// 配合 INSERT IGNORE 和 message_id 唯一索引，重复消费/重放是幂等的
type KafkaConsumer struct {
	reader *kafka.Reader
	db     *DatabaseManager
	config *MessageCenterConfig

	// 批量落库缓冲（同时保留kafka消息以便落库成功后提交offset）
	batch      []*batchEntry
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

// batchEntry 批量缓冲条目
type batchEntry struct {
	persist *PersistMessage
	kafka   kafka.Message
}

// NewKafkaConsumer 创建Kafka消费者
func NewKafkaConsumer(db *DatabaseManager, config *MessageCenterConfig) *KafkaConsumer {
	return &KafkaConsumer{
		db:     db,
		config: config,
		batch:  make([]*batchEntry, 0, config.BatchInsertSize),
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

	// 落库剩余批次并提交offset
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
		if persistMsg == nil {
			// 解析失败的消息直接跳过（无法重放也没有重试价值）
			commitCtx, commitCancel := context.WithTimeout(kc.ctx, 5*time.Second)
			if err := kc.reader.CommitMessages(commitCtx, m); err != nil {
				log.Printf("Failed to commit kafka offset: %v", err)
			}
			commitCancel()
			continue
		}

		atomic.AddInt64(&kc.messagesConsumed, 1)
		kc.addToBatch(&batchEntry{persist: persistMsg, kafka: m})
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
func (kc *KafkaConsumer) addToBatch(entry *batchEntry) {
	kc.batchMutex.Lock()
	defer kc.batchMutex.Unlock()

	kc.batch = append(kc.batch, entry)

	// 达到批量大小立即落库
	if len(kc.batch) >= kc.config.BatchInsertSize {
		batch := kc.batch
		kc.batch = make([]*batchEntry, 0, kc.config.BatchInsertSize)
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

// flushBatch 落库当前批次并提交offset
func (kc *KafkaConsumer) flushBatch() {
	kc.batchMutex.Lock()
	if len(kc.batch) == 0 {
		kc.batchMutex.Unlock()
		return
	}
	batch := kc.batch
	kc.batch = make([]*batchEntry, 0, kc.config.BatchInsertSize)
	kc.batchMutex.Unlock()

	kc.persistBatch(batch)
}

// persistBatch 执行批量落库，成功后提交offset
// 落库失败时不提交offset，消息靠重启重放恢复（at-least-once）
func (kc *KafkaConsumer) persistBatch(batch []*batchEntry) {
	persistMsgs := make([]*PersistMessage, len(batch))
	for i, entry := range batch {
		persistMsgs[i] = entry.persist
	}

	if err := kc.db.BatchInsertMessages(persistMsgs); err != nil {
		log.Printf("Failed to persist kafka message batch: %v", err)
		atomic.AddInt64(&kc.messagesFailed, int64(len(batch)))
		return
	}

	// 落库成功，提交本批所有消息的offset
	for _, entry := range batch {
		commitCtx, commitCancel := context.WithTimeout(kc.ctx, 5*time.Second)
		if err := kc.reader.CommitMessages(commitCtx, entry.kafka); err != nil {
			log.Printf("Failed to commit kafka offset: %v", err)
		}
		commitCancel()
	}

	atomic.AddInt64(&kc.messagesPersisted, int64(len(batch)))
}

// GetStats 获取消费者统计信息
func (kc *KafkaConsumer) GetStats() map[string]interface{} {
	kc.batchMutex.Lock()
	pending := len(kc.batch)
	kc.batchMutex.Unlock()

	return map[string]interface{}{
		"messages_consumed":  atomic.LoadInt64(&kc.messagesConsumed),
		"messages_persisted": atomic.LoadInt64(&kc.messagesPersisted),
		"messages_failed":    atomic.LoadInt64(&kc.messagesFailed),
		"pending_batch":      pending,
	}
}
