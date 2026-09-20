# pkg/app/types — 应用公共类型

定义 app 模块的公共数据类型，零 internal 依赖，可被任意包安全引用。

## API 速查表

### AdminConfig

内置 admin HTTP 控制面配置结构体。

```go
type AdminConfig struct {
    Disable         bool          `yaml:"disable" mapstructure:"disable"`
    ListenAddr      string        `yaml:"listen_addr" mapstructure:"listen_addr"`
    Token           string        `yaml:"token" mapstructure:"token"`
    ShutdownTimeout time.Duration `yaml:"shutdown_timeout" mapstructure:"shutdown_timeout"`
    Pprof           bool          `yaml:"pprof" mapstructure:"pprof"`
}
```

| 字段 | 说明 |
|------|------|
| `Disable` | 是否禁用 admin 控制面 |
| `ListenAddr` | 监听地址（默认 `127.0.0.1:8041`；非回环必须同时配 `Token`） |
| `Token` | 控制面鉴权令牌；**空 = 不启用鉴权**，此时 `ListenAddr` 必须是回环 |
| `ShutdownTimeout` | 关闭等待超时（默认 5s） |
| `Pprof` | 是否开启 pprof 端点 |

#### Normalize() error

填充零值字段为默认值，并校验安全边界。在启动 admin server 前调用。

**返回非 nil 表示配置不安全，调用方必须中止启动**（不是改写成回环继续跑）：

- `Token` 为空 且 `ListenAddr` 不是回环 → 返回 `ErrNonLoopbackWithoutToken`。
  控制面挂着 `/admin/shutdown`、`/admin/drain`、`/admin/gateway/upstream`、`/deadletter`、
  `/log/level`、`/debug/pprof`，无鉴权 + 非回环 = 任何同网可达者都能关服 / 切上游 /
  改日志级别 / 改死信队列。
- `Disable: true` 时不校验（该配置根本不监听）。

`Token` 非空时 `/admin/*`、`/deadletter`、`/log/level`、`/debug/pprof` 一律要求请求带同一令牌
（请求头 `X-Admin-Token` 或 `Authorization: Bearer`）。其中 `/deadletter` 整组在列
（含只读的 `/deadletter/dlq` 与 `/deadletter/pending`）：其 `retry` / `remove` 会重投或删除
跨服事件，只读分支也暴露事件体与玩家标识。

#### IsLoopbackAddr(addr string) bool

判断 `host:port` / `host` 是否只绑定回环。语义保守：空 host（所有网卡）、通配地址、
无法解析的主机名一律返回 `false`（按非回环处理）。

#### ErrNonLoopbackWithoutToken

`errors.New` 哨兵错误：未配 `Token` 却把控制面绑到非回环地址。用 `errors.Is` 判定。

### TableLoadedEvent

表加载完成事件，在 `app.PublishTableLoaded` 时广播。

```go
type TableLoadedEvent struct {
    Count   int      // 加载表数量
    Elapsed int      // 加载耗时（ms）
    Names   []string // 加载的表名列表
}
```

### ConnDisconnectEvent

连接断开事件，在玩家连接断开时触发。

```go
type ConnDisconnectEvent struct {
    ConnID   string // 连接 ID
    Owner    string // 连接归属标识
    PlayerID string // 关联玩家 ID（未绑定时为空）
}
```

### TableLoader

统一表加载接口，由引擎提供实现。

```go
type TableLoader interface {
    LoadAll(dir string) error
}
```

### 常量

| 常量 | 值 | 说明 |
|------|----|------|
| `DefaultListenAddr` | `127.0.0.1:8041` | admin 控制面默认监听地址 |
| `DefaultShutdownTimeout` | `5s` | admin 控制面默认关闭超时 |
