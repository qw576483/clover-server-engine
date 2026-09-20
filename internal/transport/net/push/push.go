// Package push 提供推送通道抽象：
//   - PushChannel（单次推送）：PlayerChannel(单播)/SceneChannel(场景广播)/AllChannel(全服广播)
//   - Push/Alert 函数。
package push

import (
	"errors"

	"github.com/qw576483/clover-server-engine/internal/domain/data"
	"github.com/qw576483/clover-server-engine/internal/shared/proto"
	ujson "github.com/qw576483/clover-server-engine/pkg/shared/json"
)

// EAlertNotify 弹窗提示内容（= proto.EAlertNotify）。
type EAlertNotify = proto.EAlertNotify

// PushChannel 推送通道抽象：把一条消息（msgID + body）推到目标。
type PushChannel interface {
	Push(msgID uint32, body []byte) error
}

// PlayerChannel 单播通道：推给指定玩家（经 NATS NotifyPush，Target=playerID）。
type PlayerChannel struct {
	Pub          data.Publisher
	Subject      string
	PlayerID     string
	TraceID      string             // 可选全链路追踪 ID（非空时写入 NotifyPush.TraceID）
	DeliveryMode proto.DeliveryMode // 传输模式：0=尽力而为，1=可靠传输，2=持久化
}

func (c PlayerChannel) Push(msgID uint32, body []byte) error {
	if c.Pub == nil {
		return errors.New("push: nil publisher")
	}
	p := proto.NotifyPush{
		Target:       c.PlayerID,
		MsgID:        msgID,
		Body:         body,
		TraceID:      c.TraceID,
		DeliveryMode: c.DeliveryMode,
	}
	buf, err := proto.EncodeNotifyPush(&p)
	if err != nil {
		return err
	}
	return c.Pub.Publish(c.Subject, buf)
}

// SceneChannel 场景广播通道：经 Scene.Broadcast 推给场景全体成员。
type SceneChannel struct {
	Scene        proto.ESceneBroadcaster
	DeliveryMode proto.DeliveryMode // 传输模式：0=尽力而为，1=可靠传输，2=持久化
}

func (c SceneChannel) Push(msgID uint32, body []byte) error {
	if c.Scene == nil {
		return errors.New("push: nil scene")
	}
	// 尝试使用带 DeliveryMode 的广播方法
	type broadcasterWithMode interface {
		BroadcastWithMode(msgID uint32, body []byte, mode proto.DeliveryMode)
	}
	if bm, ok := c.Scene.(broadcasterWithMode); ok {
		bm.BroadcastWithMode(msgID, body, c.DeliveryMode)
	} else {
		// 降级：不支持 DeliveryMode 的实现，使用默认广播
		c.Scene.Broadcast(msgID, body)
	}
	return nil
}

// AllChannel 全服广播通道：Target=TargetAll，网关广播全部在线会话。
type AllChannel struct {
	Pub          data.Publisher
	Subject      string
	TraceID      string             // 可选全链路追踪 ID
	DeliveryMode proto.DeliveryMode // 传输模式：0=尽力而为，1=可靠传输，2=持久化
}

func (c AllChannel) Push(msgID uint32, body []byte) error {
	if c.Pub == nil {
		return errors.New("push: nil publisher")
	}
	p := proto.NotifyPush{
		Target:       proto.TargetAll,
		MsgID:        msgID,
		Body:         body,
		TraceID:      c.TraceID,
		DeliveryMode: c.DeliveryMode,
	}
	buf, err := proto.EncodeNotifyPush(&p)
	if err != nil {
		return err
	}
	return c.Pub.Publish(c.Subject, buf)
}

// Push 通用数据推送：把 msgID+body 经 ch 推到目标。
// 可靠性由 ch 的 DeliveryMode 表达（PlayerChannel/SceneChannel/AllChannel 均可指定），
// 不再另设 PushReliable 之类的同名包装。
func Push(ch PushChannel, msgID uint32, body []byte) error {
	if ch == nil {
		return errors.New("push: nil channel")
	}
	return ch.Push(msgID, body)
}

// Alert 弹窗提示推送（自动以 EPushAlert 推送）。
func Alert(ch PushChannel, a *EAlertNotify) error {
	if ch == nil {
		return errors.New("push: nil channel")
	}
	body, err := ujson.Marshal(a)
	if err != nil {
		return err
	}
	return ch.Push(proto.EPushAlert, body)
}
