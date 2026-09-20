// Package pathfinding 提供寻路相关类型定义与接口。
// 实现位于 pkg/domain/mmo/mmo.go，委托给 internal 的寻路引擎。
package pathfinding

import "github.com/qw576483/clover-server-engine/pkg/domain/mmo/ai/btree"

// Point 二维整数坐标。
type Point struct{ X, Z int32 }

// PathResult 寻路结果。
type PathResult struct {
	Points     []Point
	Iterations int
}

// WalkAction 走到目标地点的行为树 Action。
type WalkAction interface {
	SetTarget(target Point)
	Tick(bb *btree.Blackboard) btree.Status
}
