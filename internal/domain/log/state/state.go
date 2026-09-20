// log 服的落盘后端契约（包说明见 wire.go）。
//
// 后端真身定义在 `pkg/foundation/logstore`：业务方据此实现自己的后端，经
// `app.RegisterLogBackend` 注册，再用配置 `log_backend` 切换 —— **换后端不必改引擎代码**。
// 引擎内置 MySQL 实现（mysql.go，写 biz_log 表）作为默认值。
package state

import "github.com/qw576483/clover-server-engine/pkg/foundation/logstore"

// LogService 引擎内部沿用的后端契约名，真身是 logstore.Backend。
type LogService = logstore.Backend
