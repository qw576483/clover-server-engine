package frame

import pframe "clover-server-engine/pkg/domain/room/frame"

// facade.go 把 internal 的具体实现适配为 pkg/domain/room/frame 的接口。
//
// 依赖方向：internal → pkg（类型真身在 pkg、实现在 internal）。
// 适配的原因只有一处：internal 的 NewRoom/Get/... 返回具体 *Room，
// 而 pkg 接口要求返回 Room 接口，Go 不支持返回类型协变，故需要这一层薄包装。

// roomFacade 把内部 *Room 适配为 pkg 的 Room 接口。
type roomFacade struct{ inner *Room }

func (w *roomFacade) ID() string                               { return w.inner.ID() }
func (w *roomFacade) Config() Config                           { return w.inner.Config() }
func (w *roomFacade) Frame() int64                             { return w.inner.Frame() }
func (w *roomFacade) Join(playerID string) error               { return w.inner.Join(playerID) }
func (w *roomFacade) Leave(playerID string) error              { return w.inner.Leave(playerID) }
func (w *roomFacade) Input(playerID string, input Input) error { return w.inner.Input(playerID, input) }
func (w *roomFacade) MarkDisconnected(playerID string) error {
	return w.inner.MarkDisconnected(playerID)
}
func (w *roomFacade) Reconnect(playerID string) (RecoveryPack, error) {
	return w.inner.Reconnect(playerID)
}
func (w *roomFacade) Recovery(playerID string) (RecoveryPack, error) {
	return w.inner.Recovery(playerID)
}
func (w *roomFacade) Snapshot() Snapshot                { return w.inner.Snapshot() }
func (w *roomFacade) ExportState() RoomState            { return w.inner.ExportState() }
func (w *roomFacade) ImportState(state RoomState) error { return w.inner.ImportState(state) }
func (w *roomFacade) Info() RoomInfo                    { return w.inner.Info() }

// serviceFacade 把内部 *Service 适配为 pkg 的 Service 接口。
type serviceFacade struct{ inner *Service }

func (w *serviceFacade) NewRoom(roomID string, opts ...Option) (pframe.Room, error) {
	r, err := w.inner.NewRoom(roomID, opts...)
	if err != nil {
		return nil, err
	}
	return &roomFacade{inner: r}, nil
}

func (w *serviceFacade) EnsureRoom(roomID string, opts ...Option) (pframe.Room, error) {
	r, err := w.inner.EnsureRoom(roomID, opts...)
	if err != nil {
		return nil, err
	}
	return &roomFacade{inner: r}, nil
}

func (w *serviceFacade) Get(roomID string) (pframe.Room, bool) {
	r, ok := w.inner.Get(roomID)
	if !ok {
		return nil, false
	}
	return &roomFacade{inner: r}, true
}

func (w *serviceFacade) MustGet(roomID string) (pframe.Room, error) {
	r, err := w.inner.MustGet(roomID)
	if err != nil {
		return nil, err
	}
	return &roomFacade{inner: r}, nil
}

func (w *serviceFacade) SetInputApplier(fn InputApplier) { w.inner.SetInputApplier(fn) }
func (w *serviceFacade) Join(roomID, playerID string) error {
	return w.inner.Join(roomID, playerID)
}
func (w *serviceFacade) Leave(roomID, playerID string) error {
	return w.inner.Leave(roomID, playerID)
}
func (w *serviceFacade) Input(roomID, playerID string, input Input) error {
	return w.inner.Input(roomID, playerID, input)
}
func (w *serviceFacade) Destroy(roomID string) error { return w.inner.Destroy(roomID) }
func (w *serviceFacade) Snapshot(roomID string) (Snapshot, error) {
	return w.inner.Snapshot(roomID)
}
func (w *serviceFacade) ExportState(roomID string) (RoomState, error) {
	return w.inner.ExportState(roomID)
}
func (w *serviceFacade) ImportState(state RoomState) (pframe.Room, error) {
	r, err := w.inner.ImportState(state)
	if err != nil {
		return nil, err
	}
	return &roomFacade{inner: r}, nil
}
func (w *serviceFacade) Recovery(roomID, playerID string) (RecoveryPack, error) {
	return w.inner.Recovery(roomID, playerID)
}
func (w *serviceFacade) Reconnect(roomID, playerID string) (RecoveryPack, error) {
	return w.inner.Reconnect(roomID, playerID)
}
func (w *serviceFacade) Disconnect(roomID, playerID string) error {
	return w.inner.Disconnect(roomID, playerID)
}
func (w *serviceFacade) Info(roomID string) (RoomInfo, error) { return w.inner.Info(roomID) }
func (w *serviceFacade) ListRooms() []string                  { return w.inner.ListRooms() }
func (w *serviceFacade) Close()                               { w.inner.Close() }
func (w *serviceFacade) Metrics() Metrics                     { return w.inner.Metrics() }

// Facade 把内部实现换成 pkg 门面接口（引擎装配时调用，业务侧只见到接口）。
func Facade(s *Service) pframe.Service {
	if s == nil {
		return nil
	}
	return &serviceFacade{inner: s}
}

// FacadeRoom 把内部房间句柄换成 pkg 的 Room 接口。
func FacadeRoom(r *Room) pframe.Room {
	if r == nil {
		return nil
	}
	return &roomFacade{inner: r}
}

// UnwrapService 还原内部实现，供引擎内部需要底层时使用。
func UnwrapService(s pframe.Service) (*Service, bool) {
	if v, ok := s.(*serviceFacade); ok {
		return v.inner, true
	}
	return nil, false
}

// 编译期断言：适配器满足 pkg 接口。
var (
	_ pframe.Service = (*serviceFacade)(nil)
	_ pframe.Room    = (*roomFacade)(nil)
)
