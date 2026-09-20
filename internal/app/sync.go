// 玩家全量数据同步器（FullSyncer）。
package app

import (
	"context"
	"database/sql"
	"encoding/json"
	"sync"

	"github.com/qw576483/clover-server-engine/internal/domain/data"
	"github.com/qw576483/clover-server-engine/internal/domain/data/accessor"
	"github.com/qw576483/clover-server-engine/internal/domain/data/account"
	"github.com/qw576483/clover-server-engine/internal/domain/data/player"
	"github.com/qw576483/clover-server-engine/internal/shared/proto"
	"github.com/qw576483/clover-server-engine/internal/transport/pubsub"
	"github.com/qw576483/clover-server-engine/pkg/foundation/logger"
	ujson "github.com/qw576483/clover-server-engine/pkg/shared/json"
)

// accountSecretTypes 不下发给客户端的密钥类 type。
var accountSecretTypes = map[string]bool{
	"cred": true, "password": true, "token": true, "pwd": true,
}

// FullSyncer 玩家全量同步器。
// 封装 PushPlayerFullSync 的全部逻辑——角色档案 + 账号信息 + 遍历监控项逐 Kind 快照。
// 构造后注入 Game 的各个依赖，通过 Push 方法执行同步。
type FullSyncer struct {
	entityAcc     *accessor.Accessor
	playerStore   *player.Store
	accountStore  *account.Store
	notifyPub     Publisher
	notifySubject string
	sessionToken  func(playerID string) string
	// monitoredData 由 Core.monitoredDataLocked 注入：调用方**必须**持有 monitorDataMu。
	monitoredData func(playerID string) map[data.OwnerType]string
	monitorDataMu *sync.RWMutex
}

// Publisher 通知发布接口（NATS 发布器的最小抽象）。
// 真身在 internal/transport/pubsub，这里只做别名——此前本文件手写了一遍同签名接口，
// 与 sync.go / mmo.go / remotexfer.go 的别名写法不一致（差一处 alias）。
type Publisher = pubsub.Publisher

// FullSyncOptions FullSyncer 的构造参数。
type FullSyncOptions struct {
	EntityAcc     *accessor.Accessor
	PlayerStore   *player.Store
	AccountStore  *account.Store
	NotifyPub     Publisher
	NotifySubject string
	SessionToken  func(playerID string) string
	MonitoredData func(playerID string) map[data.OwnerType]string
	MonitorMu     *sync.RWMutex
}

// NewFullSyncer 创建全量同步器。
func NewFullSyncer(opts FullSyncOptions) *FullSyncer {
	return &FullSyncer{
		entityAcc:     opts.EntityAcc,
		playerStore:   opts.PlayerStore,
		accountStore:  opts.AccountStore,
		notifyPub:     opts.NotifyPub,
		notifySubject: opts.NotifySubject,
		sessionToken:  opts.SessionToken,
		monitoredData: opts.MonitoredData,
		monitorDataMu: opts.MonitorMu,
	}
}

// Push 执行一次全量同步推送。
// 编排顺序：角色档案 → 账号信息 → 遍历监控表逐 Kind 快照 → 编码后经 NATS 下发。
func (fs *FullSyncer) Push(playerID, account string) {
	// notifyPub 必须在最早处一起判空：原先只在最后 publish 前判，
	// 没有发布器时仍会把画像 + 监控表全部快照一遍再丢弃（白干活）。
	if fs.entityAcc == nil || fs.notifyPub == nil || fs.notifySubject == "" {
		return
	}
	ctx := context.Background()
	// SessionToken 构造项允许缺省（NewFullSyncer 不强制），此时不下发 token，
	// 不能直接调用导致 nil 函数 panic。
	var token string
	if fs.sessionToken != nil {
		token = fs.sessionToken(playerID)
	}
	sync := proto.EPlayerFullSyncNotify{
		PlayerID:     playerID,
		Account:      account,
		Data:         map[string]map[string]json.RawMessage{},
		AccountData:  map[string]json.RawMessage{},
		SessionToken: token,
	}
	fs.collectPlayerProfile(ctx, playerID, &sync)
	fs.collectAccountProfile(ctx, account, &sync)
	fs.collectMonitoredKinds(ctx, playerID, &sync)

	body, err := ujson.Marshal(sync)
	if err != nil {
		return
	}
	// 全量同步必须使用可靠传输（丢包=玩家数据残缺）
	push := proto.NotifyPush{
		Target:       account,
		MsgID:        proto.EPushPlayerFullSync,
		Body:         body,
		DeliveryMode: proto.DeliveryModeReliable,
	}
	buf, err := proto.EncodeNotifyPush(&push)
	if err != nil {
		return
	}
	if err := fs.notifyPub.Publish(fs.notifySubject, buf); err != nil {
		logger.Errorf("FullSync.Push: publish failed: %v", err)
	}
}

func (fs *FullSyncer) collectMonitoredKinds(ctx context.Context, playerID string, sync *proto.EPlayerFullSyncNotify) {
	fs.monitorDataMu.RLock()
	monitored := fs.monitoredData(playerID)
	fs.monitorDataMu.RUnlock()
	for kind, entityID := range monitored {
		snap, err := fs.entityAcc.SnapshotClientSelf(ctx, kind, entityID)
		if err != nil {
			logger.Errorf("FullSync.Push: snapshot %s/%s failed: %v", kind, entityID, err)
			continue
		}
		if len(snap) == 0 {
			continue
		}
		switch kind {
		case data.OwnerAccount:
			for typ, raw := range snap {
				if accountSecretTypes[typ] {
					continue
				}
				sync.AccountData[typ] = raw
			}
		default:
			sync.Data[string(kind)] = snap
		}
	}
}

func (fs *FullSyncer) collectPlayerProfile(ctx context.Context, playerID string, sync *proto.EPlayerFullSyncNotify) {
	if fs.playerStore == nil {
		return
	}
	p, err := fs.playerStore.GetByID(ctx, playerID)
	if err != nil {
		logger.Errorf("FullSync.Push: get player %s failed: %v", playerID, err)
		return
	}
	sync.Player = proto.EPlayerSyncView{
		PlayerID: p.PlayerID, Name: p.Name, Account: p.Account, ServerID: p.ServerID,
	}
}

func (fs *FullSyncer) collectAccountProfile(ctx context.Context, account string, sync *proto.EPlayerFullSyncNotify) {
	if fs.accountStore == nil {
		return
	}
	a, err := fs.accountStore.Load(ctx, account)
	if err != nil {
		logger.Errorf("FullSync.Push: load account %s failed: %v", account, err)
		return
	}
	sync.AccountInfo = &proto.EAccountSyncView{
		Account: a.Account, CreateTime: nullStr(a.CreateTime), LoginTime: nullStr(a.LoginTime),
	}
}

func nullStr(v sql.NullString) string {
	if !v.Valid {
		return ""
	}
	return v.String
}
