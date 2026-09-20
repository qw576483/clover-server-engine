package app

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	authdom "clover-server-engine/internal/domain/auth"
	"clover-server-engine/internal/domain/data"
	"clover-server-engine/internal/domain/master"
	"clover-server-engine/internal/foundation/config"
	"clover-server-engine/internal/shared/proto"
	"clover-server-engine/internal/transport/etcd"
	"clover-server-engine/internal/transport/event"
	"clover-server-engine/internal/transport/nats"
	"clover-server-engine/internal/transport/net/session"
	"clover-server-engine/pkg/foundation/logger"

	"go.uber.org/zap"
)

// server_type 常量（进程角色分发，由本包 runProcess 消费）
const (
	ServerTypeGame    = "game"    // 游戏服（game 服）server_type：承载玩法与玩家状态
	ServerTypeGateway = "gateway" // 网关 server_type
	ServerTypeAll     = "all"     // 网关 + 游戏服同进程一体启动
	ServerTypeMaster  = "master"  // 顶层协调服：节点注册与摘除 / 玩家定位 / 排行榜 / SessionToken / 死节点探测
	ServerTypeLog     = "log"     // 日志服：接收 game 批量上报的业务日志并落盘
	ServerTypeAuth    = "auth"    // 账号服：注册 / 登录 / 签发 token，只暴露 HTTP（不接游戏长连接）

	defaultMasterListenAddr     = "" // master 默认 TCP 地址（空=不启用，需配置文件指定）
	defaultMasterHTTPListenAddr = "" // master 默认 HTTP 地址（空=不启用，需配置文件指定）
	defaultLogListenAddr        = "" // log 服默认 TCP 地址（空=不启用，需配置文件指定）
	defaultLogHTTPListenAddr    = "" // log 服默认 HTTP 地址（空=不启用，需配置文件指定）
)

// GatewayConfig 网关进程配置。同时支持 WebSocket 与 TCP 两种客户端接入
// （二选一或同时启用），由 ListenWS / ListenTCP 控制；为空者不启用。

// 最小化端口方案（3 个端口号收敛全部客户端接入协议）：
//   - ListenTCP：原生客户端保底（TCP 二进制协议）。
//   - ListenUDP：QUIC + 裸 UDP 共享同一 UDP socket，按首字节分发。
//   - ListenWS：网页端入口；WebSocket 走 TCP，WebTransport 复用同端口号 UDP。

// 连接层（限流 / 排队 / 重连）相关字段：
//   - MaxConns / QueueCap / QueueReleasePerSec / QueueTimeout：限流与排队（见报告 3. 连接层）。
//   - ReconnectGrace：重连宽限（保留 owner→过期记录用于标记重连）。
//   - DisconnectGrace：断线宽限（断开后延迟触发 OnDisconnect，期间软掉线→硬掉线）。
//     以上字段为 0 值时表示「不启用 / 立即触发」。
type GatewayConfig struct {
	ListenWS           string        `yaml:"listen_ws" mapstructure:"listen_ws"`                       // 客户端 WebSocket 接入地址（网页端入口；WebTransport 复用同端口号 UDP）；空=不启用 WS
	WSPath             string        `yaml:"ws_path" mapstructure:"ws_path"`                           // WebSocket 升级路径
	WSAllowAllOrigins  bool          `yaml:"ws_allow_all_origins" mapstructure:"ws_allow_all_origins"` // 允许跨域 WS 连接（msg-web 等网页测试工具需要）
	ListenTCP          string        `yaml:"listen_tcp" mapstructure:"listen_tcp"`                     // 客户端 TCP 接入地址（原生客户端保底）；空=不启用 TCP
	ListenUDP          string        `yaml:"listen_udp" mapstructure:"listen_udp"`                     // 客户端 UDP 接入地址（QUIC + 裸 UDP 共享）；空=不启用 UDP
	EnableWT           bool          `yaml:"enable_wt" mapstructure:"enable_wt"`                       // 是否在 ListenWS 端口号上启用 WebTransport（UDP 侧）；默认 true
	TLSCert            string        `yaml:"tls_cert" mapstructure:"tls_cert"`                         // TLS 证书文件路径
	TLSKey             string        `yaml:"tls_key" mapstructure:"tls_key"`                           // TLS 私钥文件路径
	WTCert             string        `yaml:"wt_cert" mapstructure:"wt_cert"`                           // WebTransport 专用证书路径；空=自动生成
	WTKey              string        `yaml:"wt_key" mapstructure:"wt_key"`                             // WebTransport 专用私钥路径；空=自动生成
	WTPin              *bool         `yaml:"wt_pin" mapstructure:"wt_pin"`
	TCPTLSDisabled     bool          `yaml:"tcp_tls_disabled" mapstructure:"tcp_tls_disabled"`           // true=TCP 接入不加密（网关开了 TLS 时保住裸 TCP 原生客户端）；false=跟随 tls_cert（默认）
	MaxConns           int           `yaml:"max_conns" mapstructure:"max_conns"`                         // 连接总数上限（活跃会话数）；0=不限制
	MaxConnsPerSec     int           `yaml:"max_conns_per_sec" mapstructure:"max_conns_per_sec"`         // 每秒新建连接数上限（连接建立限流）；0=不限制
	QueueCap           int           `yaml:"queue_cap" mapstructure:"queue_cap"`                         // 等候队列容量；>0=启用排队；0=不排队
	QueueReleasePerSec int           `yaml:"queue_release_per_sec" mapstructure:"queue_release_per_sec"` // 排队每秒放行数；0=尽快放行
	QueueTimeout       time.Duration `yaml:"queue_timeout" mapstructure:"queue_timeout"`                 // 排队最长时间；0=不限时
	ReconnectGrace     time.Duration `yaml:"reconnect_grace" mapstructure:"reconnect_grace"`             // 重连宽限（保留 owner 记录用于标记重连）；0=关闭
	DisconnectGrace    time.Duration `yaml:"disconnect_grace" mapstructure:"disconnect_grace"`           // 断线宽限（延迟触发 OnDisconnect）；0=立即触发
	// MaxFrameSize 上行客户端帧最大长度（字节）；默认 `session.MaxFrameSize`（10 MiB，见 DefaultConfig），显式配 0=不限制。
	//
	// **此值会下传到各传输层**（TCP / WS 的 ServerConfig.MaxMsgSize），是两端唯一对齐的帧上限；
	// 默认值取 `session.MaxFrameSize`（`internal/transport/net/session/crypto.go`，服务端帧上限
	// 的唯一来源，tcp / ws / quic 传输层与会话加密的密文上限都引它）；
	// 客户端 `ClientFrame.MaxBodySize` / `MaxFramePayload` / WS `MaxMsgPayload` / QUIC `MaxFrameSize`
	// 同为 `10 << 20`。改这里等于改两端契约，必须与客户端常量同步。
	// ⚠️ 调到 10 MiB 以上时，`session` 的会话加密密文上限也要一起抬，否则启用通道加密的超大帧仍发不出。
	MaxFrameSize int `yaml:"max_frame_size" mapstructure:"max_frame_size"`

	// AuthDisabled 关闭登录门禁（仅开发 / 本地调试；生产保持 false）。
	//
	// 零值 = 开启门禁（fail-safe，与 HTTPAuthDisabled 同款反向命名）：未绑定 owner 的连接
	// 只放行 AuthExemptMsgIDs 中的消息号，其余在网关侧直接拒绝（回 EErrorReply{code:401}），
	// 不转发逻辑服——无效流量挡在门口。详见 gwcore.Config.AuthDisabled。
	AuthDisabled bool `yaml:"auth_disabled" mapstructure:"auth_disabled"`
	// AuthExemptMsgIDs 免登录消息号白名单；空（默认）时用引擎内置白名单
	// （EMsgLogin / EMsgResumeSession）。配置后**完全覆盖**内置白名单（不是追加）。
	// 仅当业务确有「登录前必须可达」的消息（公告查询、版本检查等）时才需要配置。
	AuthExemptMsgIDs []uint32 `yaml:"auth_exempt_msg_ids" mapstructure:"auth_exempt_msg_ids"`
}

// MiscConfig 杂项配置。
type MiscConfig struct {
	Timezone string `yaml:"timezone" mapstructure:"timezone"` // 时区，如 "Asia/Shanghai"、"America/New_York"；空=系统本地时区
}

// LogicConfig 逻辑服配置。
type LogicConfig struct {
	ListenAddr string `yaml:"listen_addr" mapstructure:"listen_addr"` // TCP 监听地址（网关拨号此地址）
	HTTPListen string `yaml:"http_listen" mapstructure:"http_listen"` // HTTP 控制面监听地址；空=不启用
	// Heartbeat 网关↔逻辑服心跳间隔，默认 30s（**服务端主动探测**语义）。
	//
	// 与客户端心跳不是一回事，两端默认值不同（服务端 30s / 客户端 15s）是**有意为之**：
	//   - 客户端 `Game.HeartbeatIntervalMs` / `Connection.HeartbeatIntervalMs` = 15000ms：
	//     客户端主动上行保活，防 NAT / 中间设备静默断链；
	//   - 本字段 = 服务端侧的探测与判活节奏（读空闲超时按它推导）。
	// 二者语义不同、互不依赖，无需强行拉平；改这里不会影响客户端心跳周期。
	Heartbeat time.Duration `yaml:"heartbeat" mapstructure:"heartbeat"`
	// FrameTimeout 单帧**逻辑处理**上限，默认 30s。
	//
	// 与客户端 `Game.CallTimeoutSeconds = 10` 是**两个不同层级**的超时，不冲突：
	//   - 客户端 10s 是**请求级**超时（Call 等回包），超时后客户端自己放弃等待并报错；
	//   - 本字段是**服务端单帧处理预算**（handler 执行 + 回包的总预算），超时才由服务端
	//     记录慢帧并在网关侧断开/降级。
	// 二者不要求相等：客户端 10s 到期只会让该次请求失败，服务端仍会把处理做完并把回包写入
	// （客户端已不再等）；反过来服务端 30s 上限保证「卡死的 handler 不会永久占住一条连接」。
	// 若把本值改到小于客户端 10s，会出现「客户端还在等、服务端已判超时」的错位，故保持 >= 客户端超时。
	FrameTimeout time.Duration `yaml:"frame_timeout" mapstructure:"frame_timeout"`
	// ReconnectGrace 重连宽限：断线后保留连接数据等待重连，默认 30s
	ReconnectGrace time.Duration `yaml:"reconnect_grace" mapstructure:"reconnect_grace"`
}

// AuthConfig 已迁至 internal/domain/auth（配置属域级公共物，与 master / log 一致）。
// 本包只持有它作为 Config 的一个字段。

// Config clover 应用整体配置（网关 / 逻辑服共享同一结构，按需取用）。

// 各模块 yaml 只写自己关心的字段：
//   - server_type.yaml  进程角色（game | gateway | all | master | log | auth）
//   - gateway.yaml  网关 / 逻辑服监听地址（listen_ws / listen_tcp 任选）
//   - game.yaml    游戏服参数（重连宽限等）
//   - nats.yaml    NATS 队列
//   - data.yaml     通用数据存储（redis / mysql / cache / memory 四模式）
//   - etcd.yaml    服务发现（endpoints 为空=不启用）

// 存储收敛（用户强约束）：账号凭证、玩家角色、以及挂在它们身上的任意业务数据
// 统一走 pkg/domain/data 的三元键存储层（Key{Owner, ID, Type}），业务只经 Game.Data() 一个句柄读写全部。
type Config struct {
	ServerType  string             `yaml:"server_type" mapstructure:"server_type"`   // 进程角色：game | gateway | all | master | log | auth
	Tags        []string           `yaml:"tags" mapstructure:"tags"`                 // 业务标签：game 实例的角色标记，如 ["scene"] / ["room"]
	Gateway     GatewayConfig      `yaml:"gateway" mapstructure:"gateway"`           // 网关接入配置
	Logic       LogicConfig        `yaml:"logic" mapstructure:"logic"`               // 逻辑服配置
	NATS        nats.NatsConfig    `yaml:"nats" mapstructure:"nats"`                 // NATS 消息队列（Addr 空=不启用）
	Data        data.Config        `yaml:"data" mapstructure:"data"`                 // 通用数据存储
	Etcd        etcd.EtcdConfig    `yaml:"etcd" mapstructure:"etcd"`                 // 服务发现（endpoints 空=不启用）
	NATSSubject string             `yaml:"nats_subject" mapstructure:"nats_subject"` // 下行推送 subject；空=用 proto.NATSSubjectNotify 默认
	Log         logger.Config      `yaml:"log" mapstructure:"log"`                   // 日志配置
	Misc        MiscConfig         `yaml:"misc" mapstructure:"misc"`                 // 杂项配置（时区等）
	Auth        authdom.AuthConfig `yaml:"auth" mapstructure:"auth"`                 // 账号服与登录链路（listen / jwt_secret / verify_addr 等）
	Admin       AdminConfig        `yaml:"admin" mapstructure:"admin"`               // 内置 admin HTTP 控制面（health/ready/metrics/log level 等）

	// MasterHealth master 节点健康探测配置：心跳周期与 Alive→Suspect→Dead 阈值。
	// 各子项零值均会在 Normalize 时回落到默认值，可整段省略。
	MasterHealth master.HealthConfig `yaml:"master_health" mapstructure:"master_health"`

	// MasterSessionToken session token 存储后端配置（memory | redis）。
	// 与集群拓扑无关：memory 后端在 master 重启后全部失效（玩家需重新登录），
	// redis 后端由 Redis TTL 持久化。零值回落默认值，可整段省略。
	MasterSessionToken master.SessionTokenConfig `yaml:"master_session_token" mapstructure:"master_session_token"`

	// MasterShard master 分片配置（index / total）。零值=单分片，行为与未启用分片一致；
	// total>1 时各 master 只持有归属自己那一段 key 的数据，需配 etcd 才能被 game 发现。
	MasterShard master.ShardConfig `yaml:"master_shard" mapstructure:"master_shard"`

	// Reliable 跨节点事件可靠投递配置：ACK 超时、重试次数、死信队列容量。
	// 零值回落默认值，可整段省略。
	Reliable event.ReliableConfig `yaml:"reliable" mapstructure:"reliable"`

	// MasterListenAddr master 内部 RPC 监听地址（server_type=master 时生效）。
	// game 通过 MasterAddr 连接此地址。空=不启用（**不会**再被当成「绑定所有网卡 + 随机端口」）。
	//
	// 安全边界（见 internal/app/master_server.go 的 validateMasterListen）：
	// 配成非回环地址时**必须**同时配 MasterToken，否则拒绝启动。
	MasterListenAddr string `yaml:"master_listen_addr" mapstructure:"master_listen_addr"`

	// MasterToken master 内部 RPC 的共享密钥（Bearer / 共享密钥语义）。
	// 非空 ⇒ master 侧要求每条连接首帧完成 MsgAuth 握手，game 侧（CallMaster / master 客户端）
	// 必须配同一个值。空 ⇒ 只能把 MasterListenAddr 绑在回环地址上。
	MasterToken string `yaml:"master_token" mapstructure:"master_token"`

	// MasterHTTPListenAddr master HTTP 控制面监听地址（server_type=master 时生效）。
	// 接收 GMT 管理消息，独立于 game 的 HTTP 端口。空=不启用，需配置文件指定。
	MasterHTTPListenAddr string `yaml:"master_http_listen_addr" mapstructure:"master_http_listen_addr"`

	// MasterAddr game 连接 master 的地址（server_type=game/all 时生效）。
	// 空=不启用，需配置文件指定。
	MasterAddr string `yaml:"master_addr" mapstructure:"master_addr"`

	// NodeID 本节点在集群内的唯一**数字**标识，用作跨机对象迁移的 scene→node 路由键
	// （NATS subject 不允许地址里的 ':'，故用数字而非 node 地址）。
	// 0 = 未配置，跨机对象迁移不启用（SceneManager.TransferRemote 返回 ErrNoRoute）；
	// 多 game 节点部署时必须各不相同，单机部署可不配。
	NodeID uint64 `yaml:"node_id" mapstructure:"node_id"`

	// LogListenAddr log 服监听地址（server_type=log 时生效）。
	// game 通过 LogAddr 连接此地址。空=不启用，需配置文件指定。
	LogListenAddr string `yaml:"log_listen_addr" mapstructure:"log_listen_addr"`

	// LogHTTPListenAddr log 服 HTTP 控制面监听地址（server_type=log 时生效）。
	// 提供 ping / health / stats 等运维端点。空=不启用，需配置文件指定。
	LogHTTPListenAddr string `yaml:"log_http_listen_addr" mapstructure:"log_http_listen_addr"`

	// LogAddr game 连接 log 服的地址（server_type=game/all 时生效）。
	// 语义是**兜底**：启用 etcd 发现时优先按 `clover/services/log/` 前缀轮询多实例，
	// 仅当发现列表为空才回退到本地址。空=不启用，需配置文件指定。
	LogAddr string `yaml:"log_addr" mapstructure:"log_addr"`

	// LogBackend 日志落盘后端名（server_type=log 时生效）。
	// 空 = 用内置默认（logstore.DefaultBackend，即 "mysql"）。
	// 填其它名字时查 pkg/foundation/logstore 的注册表 —— 业务方可以
	// app.RegisterLogBackend("my-xxx", factory) 注册自己的实现，
	// 从而做到「量级上来了改配置换后端，不改引擎代码」。
	LogBackend string `yaml:"log_backend" mapstructure:"log_backend"`

	// LogBackendConfig 自定义落盘后端的参数（原始 YAML 子节点，引擎不解释、原样交给工厂）。
	// 内置 mysql 后端不使用它 —— 它读 data.mysql。
	LogBackendConfig map[string]any `yaml:"log_backend_config" mapstructure:"log_backend_config"`

	// MasterGame 暴露 Master 端游戏内核，供 app.Mount(app.RoleMaster, ...) 回调中直接使用
	// OnEvent / OnTimer / Store 读写等能力。
	MasterGame *MasterGame `yaml:"-" mapstructure:"-"`
	// LogGame 暴露 log 服内核，供 app.Mount(app.RoleLog, ...) 回调中直接使用。
	LogGame *LogGame `yaml:"-" mapstructure:"-"`
	// AuthGame 暴露账号服内核，供 app.Mount(app.RoleAuth, ...) 回调中直接使用。
	AuthGame *AuthGame `yaml:"-" mapstructure:"-"`
}

// DefaultConfig 返回开发基线配置。
func DefaultConfig() *Config {
	// data 默认走 TierRedisMySQL（Redis 缓存 + MySQL 持久化），开发期自动建表。
	// MySQL 的 host / port / user / db_name / parse_time 不再在这里重复硬编码——
	// data.DefaultConfig() 内部已经用 mysql.DefaultConfig() 给出同一套开发基线值，
	// 两处各写一遍只会在改默认值时漂移。
	d := data.DefaultConfig()
	d.AutoCreateTable = true

	// 安全提醒：此处为「开发基线」默认值（root / 无密码 / 本地）。生产务必通过配置文件覆盖，
	// 切勿在线上使用默认凭据，否则存在数据库被直接访问的风险。
	logger.Warnf("app: using default DEV config (MySQL user=root, no password) - DO NOT use in production")

	return &Config{
		// MaxFrameSize 默认 10 MiB：取 session.MaxFrameSize（服务端帧上限唯一来源，
		// 与 tcp/ws/quic 传输层、会话加密密文上限同值；改一处必须改另一处，见字段注释）。
		Gateway: GatewayConfig{ListenWS: "", WSPath: "/ws", EnableWT: true, MaxFrameSize: session.MaxFrameSize},
		Logic: LogicConfig{
			ListenAddr:     "",
			Heartbeat:      30 * time.Second,
			FrameTimeout:   30 * time.Second,
			ReconnectGrace: 30 * time.Second,
		},
		NATS:        nats.NatsConfig{}, // 空 Addr = 不启用 NATS
		Data:        d,
		NATSSubject: proto.NATSSubjectNotify,
		Log:         *logger.DefaultConfig(),
		// admin 控制面默认启用并绑定回环地址；需要关闭时在 yaml 中配 admin.disable: true。
		Admin: AdminConfig{
			ListenAddr:      DefaultListenAddr,
			ShutdownTimeout: DefaultShutdownTimeout,
		},
		// master 节点健康探测、session token 后端与跨节点可靠投递，
		// 均由各自的 Normalize 填充默认值，保证零值配置也能跑。
		MasterHealth:       master.HealthConfig{}.Normalize(),
		MasterSessionToken: master.SessionTokenConfig{}.Normalize(),
		MasterShard:        master.ShardConfig{}.Normalize(),
		Reliable:           event.DefaultReliableConfig(),
	}
}

// LoadFromFile 从单个 YAML/JSON/TOML 配置文件加载配置。
func LoadFromFile(path string) (*Config, error) {
	loader := config.NewFileLoader(path)
	// 关闭失败属异常（句柄泄漏信号），但不影响本次加载结果 —— 留日志不改变返回。
	defer func() {
		if cerr := loader.Close(); cerr != nil {
			logger.Warnf("app: close config loader (%s): %v", path, cerr)
		}
	}()

	cfg := DefaultConfig()
	if err := loader.Load(cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}

// LoadFromDir 从配置目录加载配置：读取目录下所有 *.yaml / *.yml 文件，
// 按文件名排序后依次合并进同一 Config 结构（各模块 yaml 只负责自己关心的字段）。
func LoadFromDir(dir string) (*Config, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("app: read dir %s: %w", dir, err)
	}

	var files []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := strings.ToLower(e.Name())
		if strings.HasSuffix(name, ".yaml") || strings.HasSuffix(name, ".yml") {
			files = append(files, filepath.Join(dir, e.Name()))
		}
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("app: no yaml files found in %s", dir)
	}
	sort.Strings(files) // 确定性加载顺序

	cfg := DefaultConfig()
	for _, f := range files {
		// 用闭包 + defer 收口 loader 生命周期：与 LoadFromFile 一致，
		// 将来在 Load 前后加分支也不会漏掉 Close。
		if err := func() error {
			loader := config.NewFileLoader(f)
			defer func() { _ = loader.Close() }()
			return loader.Load(cfg)
		}(); err != nil {
			return nil, fmt.Errorf("app: load %s: %w", f, err)
		}
	}
	return cfg, nil
}

// LoadConfig 加载服务配置（自动识别目录/单文件），供业务层在 app.Run 前注入
// MasterBusiness 等回调用到。
func LoadConfig(configPath string) (*Config, error) {
	return loadConfig(configPath)
}

// loadConfig 自动识别配置路径：目录则按模块合并加载，文件则单文件加载。
// TierMemory 仅允许代码层通过 data.MemoryConfig() 编程创建（如内存房间），
// 禁止写在 YAML 配置中。
func loadConfig(configPath string) (*Config, error) {
	info, err := os.Stat(configPath)
	if err != nil {
		return nil, err
	}
	var cfg *Config
	if info.IsDir() {
		cfg, err = LoadFromDir(configPath)
	} else {
		cfg, err = LoadFromFile(configPath)
	}
	if err != nil {
		return nil, err
	}
	if cfg.Data.Tier == data.TierMemory {
		return nil, fmt.Errorf("app: data.tier \"TierMemory\" is not allowed in YAML config (use data.MemoryConfig() programmatically for in-memory rooms)")
	}
	// 配置脱敏：打印加载后的配置前先经 config.Mask 深拷贝并遮蔽敏感字段
	// （password / token / secret 等，以及 DSN 中的密码），确保日志/审计轨迹中不泄露明文凭据。
	//
	// 默认词表不含 "pass"：NatsConfig.Pass / RedisConfig.Pass / SentinelPass /
	// MySQLConfig.Pass 归一化后正是这个名字（词表按子串匹配），不补的话
	// 下面这行会把数据库 / Redis / NATS 口令明文打进日志。
	config.AddSensitiveKeys("pass")
	if masked := config.Mask(cfg); masked != nil {
		logger.Infof("config loaded (sensitive fields masked)", zap.String("config", fmt.Sprintf("%+v", masked)))
	} else {
		logger.Infof("config loaded")
	}
	return cfg, nil
}
