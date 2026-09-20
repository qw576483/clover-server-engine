package room

import (
	"encoding/json"
	"fmt"

	iframe "clover-server-engine/internal/domain/room/frame"
	proom "clover-server-engine/pkg/domain/room"
	"clover-server-engine/pkg/foundation/logger"
)

// frameKernel 把引擎内置的帧同步服务适配成 room.Kernel。
//
// 它是「可插拔内核」里的内置那一款：业务不传 Config.Kernel 时，NewModule 用它包装
// frame.Service。有了这层适配，外壳（Module）看到的始终是统一的内核接口，
// 不需要知道内核是帧同步还是状态同步。
type frameKernel struct {
	svc *iframe.Service
}

// newFrameKernel 包装一个已构造的帧同步服务。
func newFrameKernel(svc *iframe.Service) *frameKernel {
	return &frameKernel{svc: svc}
}

var _ proom.Kernel = (*frameKernel)(nil)

func (k *frameKernel) EnsureRoom(roomID string) error {
	if k == nil || k.svc == nil {
		logger.Warnf("room: frame kernel ensure room=%s rejected: service not constructed", roomID)
		return fmt.Errorf("room: 帧同步内核未构造")
	}
	if roomID == "" {
		logger.Warnf("room: frame kernel ensure rejected: empty room id")
		return fmt.Errorf("room: 房间 ID 不能为空")
	}
	if _, err := k.svc.EnsureRoom(roomID); err != nil {
		logger.Warnf("room: frame kernel ensure room=%s failed: %v", roomID, err)
		return err
	}
	return nil
}

func (k *frameKernel) Join(roomID, playerID string) error {
	if k == nil || k.svc == nil {
		logger.Warnf("room: frame kernel join room=%s player=%s rejected: service not constructed", roomID, playerID)
		return fmt.Errorf("room: 帧同步内核未构造")
	}
	if err := k.svc.Join(roomID, playerID); err != nil {
		logger.Warnf("room: frame kernel join room=%s player=%s failed: %v", roomID, playerID, err)
		return err
	}
	return nil
}

func (k *frameKernel) Leave(roomID, playerID string) error {
	if k == nil || k.svc == nil {
		logger.Warnf("room: frame kernel leave room=%s player=%s rejected: service not constructed", roomID, playerID)
		return fmt.Errorf("room: 帧同步内核未构造")
	}
	if err := k.svc.Leave(roomID, playerID); err != nil {
		logger.Warnf("room: frame kernel leave room=%s player=%s failed: %v", roomID, playerID, err)
		return err
	}
	return nil
}

func (k *frameKernel) Destroy(roomID string) error {
	if k == nil || k.svc == nil {
		logger.Warnf("room: frame kernel destroy room=%s rejected: service not constructed", roomID)
		return fmt.Errorf("room: 帧同步内核未构造")
	}
	if err := k.svc.Destroy(roomID); err != nil {
		logger.Warnf("room: frame kernel destroy room=%s failed: %v", roomID, err)
		return err
	}
	return nil
}

// ExportState 导出帧房间运行态；Recovery 由运行态派生（快照 + 追帧增量）。
func (k *frameKernel) ExportState(roomID string) (proom.ExportPack, error) {
	if k == nil || k.svc == nil {
		logger.Warnf("room: frame kernel export room=%s rejected: service not constructed", roomID)
		return proom.ExportPack{}, fmt.Errorf("room: 帧同步内核未构造")
	}
	state, err := k.svc.ExportState(roomID)
	if err != nil {
		logger.Warnf("room: frame kernel export room=%s failed: %v", roomID, err)
		return proom.ExportPack{}, err
	}
	raw, err := json.Marshal(state)
	if err != nil {
		logger.Errorf("room: frame kernel marshal state room=%s failed: %v", roomID, err)
		return proom.ExportPack{}, err
	}

	pack := proom.ExportPack{State: raw}
	rec := iframe.BuildTakeoverRecovery(state)
	pack.Frame = rec.RecoveredUntil
	pack.Hash = rec.FrameHash
	// 恢复包序列化失败不阻断接管：State 仍可恢复运行态，只是客户端少一次快照下发。
	if recRaw, err := json.Marshal(rec); err != nil {
		logger.Warnf("room: frame kernel marshal recovery room=%s failed: %v", roomID, err)
	} else {
		pack.Recovery = recRaw
	}
	return pack, nil
}

func (k *frameKernel) ImportState(roomID string, state json.RawMessage) error {
	if k == nil || k.svc == nil {
		logger.Warnf("room: frame kernel import room=%s rejected: service not constructed", roomID)
		return fmt.Errorf("room: 帧同步内核未构造")
	}
	if len(state) == 0 {
		logger.Warnf("room: frame kernel import room=%s rejected: empty state", roomID)
		return fmt.Errorf("room: 房间状态为空")
	}
	var rs iframe.RoomState
	if err := json.Unmarshal(state, &rs); err != nil {
		logger.Errorf("room: frame kernel unmarshal state room=%s failed: %v", roomID, err)
		return err
	}
	if rs.RoomID == "" {
		rs.RoomID = roomID
	}
	if _, err := k.svc.ImportState(rs); err != nil {
		logger.Warnf("room: frame kernel import room=%s failed: %v", roomID, err)
		return err
	}
	return nil
}

func (k *frameKernel) Players(roomID string) []string {
	if k == nil || k.svc == nil {
		logger.Warnf("room: frame kernel players room=%s rejected: service not constructed", roomID)
		return nil
	}
	info, err := k.svc.Info(roomID)
	if err != nil {
		logger.Warnf("room: frame kernel players room=%s failed: %v", roomID, err)
		return nil
	}
	return info.Players
}

func (k *frameKernel) Close() {
	if k == nil || k.svc == nil {
		return
	}
	k.svc.Close()
}
