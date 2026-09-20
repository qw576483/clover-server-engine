// Package sync 客户端预测 + 服务端和解（reconciliation）。
package sync

import "sync"

// PredictionStrategy 客户端预测同步策略。
// 客户端在本地执行指令 → 收到服务端权威状态后比对并修正。
type PredictionStrategy struct {
	mu              sync.Mutex
	pendingInputs   []inputEntry // 未确认的输入队列
	lastServerState State        // 最新服务端权威状态
	predictedState  State        // 当前预测状态
	valid           bool
	maxPending      int // 最大待确认输入数
}

// inputEntry 记录客户端输入及其时间。
type inputEntry struct {
	Time       int64
	VelX       float32
	VelY       float32
	VelZ       float32
	ServerTick uint64
}

// InputEntry 是 inputEntry 的导出别名：SyncManager.AddInput 要用它收输入，
// 而调用方在包外无法构造一个未导出的结构体字面量。
type InputEntry = inputEntry

// NewPrediction 创建客户端预测策略。
// maxPending: 最大待确认输入数（默认 10）。
func NewPrediction(maxPending int) *PredictionStrategy {
	if maxPending <= 0 {
		maxPending = 10
	}
	return &PredictionStrategy{maxPending: maxPending}
}

// AddInput 记录客户端输入。
func (ps *PredictionStrategy) AddInput(input inputEntry) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.pendingInputs = append(ps.pendingInputs, input)
	if len(ps.pendingInputs) > ps.maxPending {
		ps.pendingInputs = ps.pendingInputs[len(ps.pendingInputs)-ps.maxPending:]
	}
}

// AddSnapshot 接收服务端权威状态。
func (ps *PredictionStrategy) AddSnapshot(state State) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	ps.lastServerState = state

	if ps.valid {
		// 和解：移除服务端已确认的输入
		ps.reconcile(state)
	}
	ps.valid = true
}

// reconcile 将服务端已确认的输入从队列移除并修正预测状态。
func (ps *PredictionStrategy) reconcile(serverState State) {
	// 移除 serverTick 之前的输入
	cutoff := -1
	for i, inp := range ps.pendingInputs {
		if inp.ServerTick >= serverState.ServerTick {
			break
		}
		cutoff = i
	}
	if cutoff >= 0 {
		ps.pendingInputs = ps.pendingInputs[cutoff+1:]
	}

	// 在服务端权威状态基础上重放未确认的输入
	ps.predictedState = serverState
	// ★ 时间基线必须与 inp.Time 同量纲（毫秒）。
	// 此前用 int64(serverState.ServerTick)（tick **序号**）当基线，
	// 与毫秒时间戳相减得到的 dt 是一个天文数字，重放位移完全错误。
	prevTime := serverState.Time
	if prevTime <= 0 && len(ps.pendingInputs) > 0 {
		// 服务端快照没带毫秒时间戳：退化为以首条输入自身时间为基线，
		// 其 dt 会被下面的 dt<=0 分支兜成默认帧间隔，后续仍能按真实间隔推进。
		prevTime = ps.pendingInputs[0].Time
	}
	for _, inp := range ps.pendingInputs {
		// 计算相邻输入之间的时间差（毫秒转秒），防止 dt 为零或负值。
		dt := float32(inp.Time-prevTime) / 1000.0
		if dt <= 0 {
			dt = 1.0 / 60.0 // 回退到默认帧间隔（约 16.67ms）
		}
		ps.predictedState.PosX += inp.VelX * dt
		ps.predictedState.PosY += inp.VelY * dt
		ps.predictedState.PosZ += inp.VelZ * dt
		ps.predictedState.VelX = inp.VelX
		ps.predictedState.VelY = inp.VelY
		ps.predictedState.VelZ = inp.VelZ
		prevTime = inp.Time
	}
}

// Interpolate 返回当前预测状态。
func (ps *PredictionStrategy) Interpolate(nowMS int64) (State, bool) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	if !ps.valid {
		return State{}, false
	}

	// 返回预测状态（若为空则返回服务端状态）
	if ps.predictedState.Time > 0 {
		ps.predictedState.Time = nowMS
		return ps.predictedState, true
	}
	return ps.lastServerState, true
}

// Reset 清空状态。
func (ps *PredictionStrategy) Reset() {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.pendingInputs = nil
	ps.valid = false
}
