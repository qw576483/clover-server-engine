package sync

import (
	pkgsync "clover-server-engine/pkg/domain/mmo/sync"
	"clover-server-engine/pkg/foundation/logger"
)

type State = pkgsync.State
type SyncMode = pkgsync.SyncMode
type Syncer = pkgsync.Syncer

const (
	ModeInterpolation = pkgsync.ModeInterpolation
	ModeExtrapolation = pkgsync.ModeExtrapolation
	ModePrediction    = pkgsync.ModePrediction
)

func LerpState(a, b State, t float32) State { return pkgsync.LerpState(a, b, t) }

type SyncManager struct {
	mode   SyncMode
	syncer Syncer
}

func NewSyncManager(mode SyncMode) *SyncManager {
	switch mode {
	case ModeExtrapolation:
		return &SyncManager{mode: mode, syncer: NewExtrapolation(200)}
	case ModePrediction:
		return &SyncManager{mode: mode, syncer: NewPrediction(10)}
	case ModeInterpolation:
		return &SyncManager{mode: mode, syncer: NewInterpolation(100, 6)}
	default:
		// 未知 SyncMode 静默当插值处理：配置写错时行为被悄悄替换，
		// 表现为"明明配了预测却没有预测"，线上无从排查。
		logger.Warnf("sync: unknown SyncMode %d, 回落为插值策略", int(mode))
		return &SyncManager{mode: mode, syncer: NewInterpolation(100, 6)}
	}
}

func (sm *SyncManager) AddSnapshot(state State) { sm.syncer.AddSnapshot(state) }

// AddInput 记录一条客户端输入（仅预测模式有效，其余模式为空操作并留痕）。
//
// 没有这个入口，SyncManager 在 ModePrediction 下永远收不到输入：
// PredictionStrategy.reconcile 每帧重放一个空队列，预测功能形同旁路。
func (sm *SyncManager) AddInput(input InputEntry) {
	ps, ok := sm.syncer.(*PredictionStrategy)
	if !ok || ps == nil {
		logger.Warnf("sync: AddInput 被调到非预测策略上（mode=%d），输入被丢弃", int(sm.mode))
		return
	}
	if sm.mode != ModePrediction {
		logger.Warnf("sync: mode=%d 收到客户端输入，只有 ModePrediction 会消费它", int(sm.mode))
	}
	ps.AddInput(input)
}

func (sm *SyncManager) Interpolate(nowMS int64) (State, bool) { return sm.syncer.Interpolate(nowMS) }

func (sm *SyncManager) Reset() { sm.syncer.Reset() }

func (sm *SyncManager) Mode() SyncMode { return sm.mode }
