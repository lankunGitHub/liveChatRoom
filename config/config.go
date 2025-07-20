package config

import (
	"fmt"
	"liveChatroom/util/pool"
	"os"
	"time"

	"gopkg.in/yaml.v2"
)

// GlobalConfig 全局配置结构
type GlobalConfig struct {
	Log           LogConfig           `yaml:"log"`
	Server        ServerConfig        `yaml:"server"`
	Router        RouterConfig        `yaml:"router"`
	MessageCenter MessageCenterConfig `yaml:"messageCenter"`
	Client        ClientConfig        `yaml:"client"`
	Utils         UtilsConfig         `yaml:"utils"`
}

// LogConfig 日志配置
type LogConfig struct {
	Level      int    `yaml:"level"`      // 日志级别 (0=Debug, 1=Info, 2=Warn, 3=Error)
	Output     string `yaml:"output"`     // 输出方式 (console/file)
	TimeFormat string `yaml:"timeFormat"` // 时间格式
}

// ServerConfig 连接服务器配置
type ServerConfig struct {
	Addr         string             `yaml:"addr"`         // 服务器监听地址
	ReadTimeout  time.Duration      `yaml:"readTimeout"`  // 读取超时
	WriteTimeout time.Duration      `yaml:"writeTimeout"` // 写入超时
	Redis        RedisConfig        `yaml:"redis"`        // Redis配置
	MySQL        MySQLConfig        `yaml:"mysql"`        // MySQL配置
	TimeWheel    TimeWheelConfig    `yaml:"timeWheel"`    // 时间轮配置
	MessageRetry MessageRetryConfig `yaml:"messageRetry"` // 消息重试配置
}

// RouterConfig 路由服务器配置
type RouterConfig struct {
	Addr         string             `yaml:"addr"`         // 路由服务器监听地址
	Redis        RedisConfig        `yaml:"redis"`        // Redis配置
	MySQL        MySQLConfig        `yaml:"mysql"`        // MySQL配置
	Kafka        KafkaConfig        `yaml:"kafka"`        // Kafka配置
	LoadCheck    LoadCheckConfig    `yaml:"loadCheck"`    // 负载检查配置
	TimeWheel    TimeWheelConfig    `yaml:"timeWheel"`    // 时间轮配置
	MessageRetry MessageRetryConfig `yaml:"messageRetry"` // 消息重试配置
}

// MessageCenterConfig 消息中心配置
type MessageCenterConfig struct {
	Kafka         KafkaConfig   `yaml:"kafka"`         // Kafka配置
	MySQL         MySQLConfig   `yaml:"mysql"`         // MySQL配置
	Redis         RedisConfig   `yaml:"redis"`         // Redis配置
	BatchSize     int           `yaml:"batchSize"`     // 批处理大小
	FlushInterval time.Duration `yaml:"flushInterval"` // 刷新间隔
	WorkerCount   int           `yaml:"workerCount"`   // 工作协程数
	RetryTimes    int           `yaml:"retryTimes"`    // 重试次数
	MessageTTL    time.Duration `yaml:"messageTTL"`    // 消息保留时间
}

// ClientConfig 客户端配置
type ClientConfig struct {
	ServerAddrs       []string      `yaml:"serverAddrs"`       // 服务器地址列表
	ConnectTimeout    time.Duration `yaml:"connectTimeout"`    // 连接超时
	ReconnectInterval time.Duration `yaml:"reconnectInterval"` // 重连间隔
	MessageTimeout    time.Duration `yaml:"messageTimeout"`    // 消息超时
	MaxRetries        int           `yaml:"maxRetries"`        // 最大重试次数
	HeartbeatInterval time.Duration `yaml:"heartbeatInterval"` // 心跳间隔
	BufferSize        int           `yaml:"bufferSize"`        // 消息缓冲区大小

	// 扩展配置（兼容现有代码）
	FetchTimeout    time.Duration `yaml:"fetchTimeout"`    // 拉取超时
	FetchBatchSize  int           `yaml:"fetchBatchSize"`  // 拉取批大小
	FetchMaxRetries int           `yaml:"fetchMaxRetries"` // 拉取最大重试次数
}

// UtilsConfig 工具库配置
type UtilsConfig struct {
	Queue QueueConfig `yaml:"queue"` // 队列配置
	Pool  PoolConfig  `yaml:"pool"`  // 池配置
}

// RedisConfig Redis配置
type RedisConfig struct {
	Addr     string `yaml:"addr"`     // Redis地址
	Password string `yaml:"password"` // Redis密码
	DB       int    `yaml:"db"`       // Redis数据库
}

// MySQLConfig MySQL配置
type MySQLConfig struct {
	DSN             string        `yaml:"dsn"`             // 数据源名称
	MaxOpenConns    int           `yaml:"maxOpenConns"`    // 最大打开连接数
	MaxIdleConns    int           `yaml:"maxIdleConns"`    // 最大空闲连接数
	ConnMaxLifetime time.Duration `yaml:"connMaxLifetime"` // 连接最大生存时间
}

// KafkaConfig Kafka配置
type KafkaConfig struct {
	Addr    string `yaml:"addr"`    // Kafka地址
	Topic   string `yaml:"topic"`   // 消息主题
	GroupID string `yaml:"groupId"` // 消费者组ID
}

// LoadCheckConfig 负载检查配置
type LoadCheckConfig struct {
	Interval    time.Duration `yaml:"interval"`    // 检查间隔
	NodeTimeout time.Duration `yaml:"nodeTimeout"` // 节点超时时间
}

// TimeWheelConfig 时间轮配置
type TimeWheelConfig struct {
	TimeTick time.Duration `yaml:"timeTick"` // 时间精度
	SlotNum  int           `yaml:"slotNum"`  // 槽位数量
}

// MessageRetryConfig 消息重试配置
type MessageRetryConfig struct {
	RetryTime time.Duration `yaml:"retryTime"` // 重试间隔
	RetryNum  int           `yaml:"retryNum"`  // 重试次数
	DelTime   time.Duration `yaml:"delTime"`   // 消息删除时间
}

// QueueConfig 队列配置
type QueueConfig struct {
	FilePath    string `yaml:"filePath"`    // 队列文件路径
	SegmentSize int    `yaml:"segmentSize"` // 段大小
	Monitor     bool   `yaml:"monitor"`     // 是否启用监控
	LatencySize int    `yaml:"latencySize"` // 延迟统计大小
}

// PoolConfig 池配置
type PoolConfig struct {
	FilePath      string `yaml:"filePath"`      // 池文件路径
	GroupSize     int    `yaml:"groupSize"`     // 组大小
	MinBufferSize int    `yaml:"minBufferSize"` // 最小缓冲区大小
}

// 全局配置实例
var GlobalConf *GlobalConfig

// LoadConfig 加载配置文件
func LoadConfig(configPath string) (*GlobalConfig, error) {
	// 读取配置文件
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %v", err)
	}

	// 解析YAML
	var config GlobalConfig
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %v", err)
	}

	// 验证配置
	if err := validateConfig(&config); err != nil {
		return nil, fmt.Errorf("config validation failed: %v", err)
	}

	// 设置全局配置
	GlobalConf = &config

	return &config, nil
}

// MustLoadConfig 加载配置文件，失败时panic
func MustLoadConfig(configPath string) *GlobalConfig {
	config, err := LoadConfig(configPath)
	if err != nil {
		panic(fmt.Sprintf("Failed to load config: %v", err))
	}
	return config
}

// validateConfig 验证配置
func validateConfig(config *GlobalConfig) error {
	// 验证服务器配置
	if config.Server.Addr == "" {
		return fmt.Errorf("server.addr is required")
	}
	if config.Server.ReadTimeout <= 0 {
		config.Server.ReadTimeout = 30 * time.Second
	}
	if config.Server.WriteTimeout <= 0 {
		config.Server.WriteTimeout = 30 * time.Second
	}

	// 验证路由配置
	if config.Router.Addr == "" {
		return fmt.Errorf("router.addr is required")
	}

	// 验证Redis配置
	if err := validateRedisConfig(&config.Server.Redis); err != nil {
		return fmt.Errorf("server.redis: %v", err)
	}
	if err := validateRedisConfig(&config.Router.Redis); err != nil {
		return fmt.Errorf("router.redis: %v", err)
	}
	if err := validateRedisConfig(&config.MessageCenter.Redis); err != nil {
		return fmt.Errorf("messageCenter.redis: %v", err)
	}

	// 验证MySQL配置
	if err := validateMySQLConfig(&config.Server.MySQL); err != nil {
		return fmt.Errorf("server.mysql: %v", err)
	}
	if err := validateMySQLConfig(&config.Router.MySQL); err != nil {
		return fmt.Errorf("router.mysql: %v", err)
	}
	if err := validateMySQLConfig(&config.MessageCenter.MySQL); err != nil {
		return fmt.Errorf("messageCenter.mysql: %v", err)
	}

	// 验证Kafka配置
	if err := validateKafkaConfig(&config.Router.Kafka); err != nil {
		return fmt.Errorf("router.kafka: %v", err)
	}
	if err := validateKafkaConfig(&config.MessageCenter.Kafka); err != nil {
		return fmt.Errorf("messageCenter.kafka: %v", err)
	}

	// 验证客户端配置
	if len(config.Client.ServerAddrs) == 0 {
		return fmt.Errorf("client.serverAddrs is required")
	}

	// 设置默认值
	setDefaults(config)

	return nil
}

// validateRedisConfig 验证Redis配置
func validateRedisConfig(config *RedisConfig) error {
	if config.Addr == "" {
		return fmt.Errorf("addr is required")
	}
	return nil
}

// validateMySQLConfig 验证MySQL配置
func validateMySQLConfig(config *MySQLConfig) error {
	if config.DSN == "" {
		return fmt.Errorf("dsn is required")
	}
	if config.MaxOpenConns <= 0 {
		config.MaxOpenConns = 10
	}
	if config.MaxIdleConns <= 0 {
		config.MaxIdleConns = 5
	}
	if config.ConnMaxLifetime <= 0 {
		config.ConnMaxLifetime = time.Hour
	}
	return nil
}

// validateKafkaConfig 验证Kafka配置
func validateKafkaConfig(config *KafkaConfig) error {
	if config.Addr == "" {
		return fmt.Errorf("addr is required")
	}
	if config.Topic == "" {
		return fmt.Errorf("topic is required")
	}
	if config.GroupID == "" {
		return fmt.Errorf("groupId is required")
	}
	return nil
}

// setDefaults 设置默认值
func setDefaults(config *GlobalConfig) {
	// 日志默认值
	if config.Log.TimeFormat == "" {
		config.Log.TimeFormat = "2006-01-02 15:04:05"
	}
	if config.Log.Output == "" {
		config.Log.Output = "console"
	}

	// 时间轮默认值
	setTimeWheelDefaults(&config.Server.TimeWheel)
	setTimeWheelDefaults(&config.Router.TimeWheel)

	// 消息重试默认值
	setMessageRetryDefaults(&config.Server.MessageRetry)
	setMessageRetryDefaults(&config.Router.MessageRetry)

	// 客户端默认值
	if config.Client.ConnectTimeout <= 0 {
		config.Client.ConnectTimeout = 10 * time.Second
	}
	if config.Client.ReconnectInterval <= 0 {
		config.Client.ReconnectInterval = 5 * time.Second
	}
	if config.Client.MessageTimeout <= 0 {
		config.Client.MessageTimeout = 10 * time.Second
	}
	if config.Client.MaxRetries <= 0 {
		config.Client.MaxRetries = 3
	}
	if config.Client.HeartbeatInterval <= 0 {
		config.Client.HeartbeatInterval = 30 * time.Second
	}
	if config.Client.BufferSize <= 0 {
		config.Client.BufferSize = 1000
	}
	if config.Client.FetchTimeout <= 0 {
		config.Client.FetchTimeout = 5 * time.Second
	}
	if config.Client.FetchBatchSize <= 0 {
		config.Client.FetchBatchSize = 50
	}
	if config.Client.FetchMaxRetries <= 0 {
		config.Client.FetchMaxRetries = 3
	}

	// 消息中心默认值
	if config.MessageCenter.BatchSize <= 0 {
		config.MessageCenter.BatchSize = 100
	}
	if config.MessageCenter.FlushInterval <= 0 {
		config.MessageCenter.FlushInterval = 5 * time.Second
	}
	if config.MessageCenter.WorkerCount <= 0 {
		config.MessageCenter.WorkerCount = 4
	}
	if config.MessageCenter.RetryTimes <= 0 {
		config.MessageCenter.RetryTimes = 3
	}
	if config.MessageCenter.MessageTTL <= 0 {
		config.MessageCenter.MessageTTL = 168 * time.Hour // 7天
	}

	// 负载检查默认值
	if config.Router.LoadCheck.Interval <= 0 {
		config.Router.LoadCheck.Interval = 30 * time.Second
	}
	if config.Router.LoadCheck.NodeTimeout <= 0 {
		config.Router.LoadCheck.NodeTimeout = 60 * time.Second
	}
}

// setTimeWheelDefaults 设置时间轮默认值
func setTimeWheelDefaults(config *TimeWheelConfig) {
	if config.TimeTick <= 0 {
		config.TimeTick = 100 * time.Millisecond
	}
	if config.SlotNum <= 0 {
		config.SlotNum = 1024
	}
}

// setMessageRetryDefaults 设置消息重试默认值
func setMessageRetryDefaults(config *MessageRetryConfig) {
	if config.RetryTime <= 0 {
		config.RetryTime = 3 * time.Second
	}
	if config.RetryNum <= 0 {
		config.RetryNum = 3
	}
	if config.DelTime <= 0 {
		config.DelTime = time.Hour
	}
}

// GetServerConfig 获取服务器配置
func GetServerConfig() ServerConfig {
	if GlobalConf == nil {
		panic("Global config not loaded")
	}
	return GlobalConf.Server
}

// GetRouterConfig 获取路由配置
func GetRouterConfig() RouterConfig {
	if GlobalConf == nil {
		panic("Global config not loaded")
	}
	return GlobalConf.Router
}

// GetMessageCenterConfig 获取消息中心配置
func GetMessageCenterConfig() MessageCenterConfig {
	if GlobalConf == nil {
		panic("Global config not loaded")
	}
	return GlobalConf.MessageCenter
}

// GetClientConfig 获取客户端配置
func GetClientConfig() ClientConfig {
	if GlobalConf == nil {
		panic("Global config not loaded")
	}
	return GlobalConf.Client
}

// GetLogConfig 获取日志配置
func GetLogConfig() LogConfig {
	if GlobalConf == nil {
		panic("Global config not loaded")
	}
	return GlobalConf.Log
}

// ToGoPoolOption 将池配置转换为GoPool选项
func (c *PoolConfig) ToGoPoolOption() pool.GoPoolOption {
	return pool.GoPoolOption{
		Capacity:    c.GroupSize * 10, // 基于组大小计算容量
		MinWorkers:  c.GroupSize,      // 最小工作协程数
		MaxWorkers:  c.GroupSize * 2,  // 最大工作协程数
		QueueSize:   c.MinBufferSize,  // 任务队列大小
		IdleTimeout: 5 * time.Minute,  // 空闲超时
		EnableStats: true,             // 启用统计
	}
}

// IsDebugLevel 检查是否为调试级别
func (c *LogConfig) IsDebugLevel() bool {
	return c.Level == 0
}

// IsInfoLevel 检查是否为信息级别
func (c *LogConfig) IsInfoLevel() bool {
	return c.Level <= 1
}

// IsWarnLevel 检查是否为警告级别
func (c *LogConfig) IsWarnLevel() bool {
	return c.Level <= 2
}

// IsErrorLevel 检查是否为错误级别
func (c *LogConfig) IsErrorLevel() bool {
	return c.Level <= 3
}
