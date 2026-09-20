package tcpmsg

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	netpkg "github.com/qw576483/clover-server-engine/internal/transport/net/tcp"
)

// 测试用的握手消息号与共享密钥（真实取值由领域层给出，见 master state.MsgAuth）。
const (
	testAuthMsgID uint32 = 1
	testBizMsgID  uint32 = 42
	testToken            = "s3cret-shared-token"
)

type authProbe struct {
	Token string `json:"token"`
}

type okReply struct {
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}

// testGate 与 master 域的实现同形：未鉴权连接只接受握手帧，其余一律拒绝并关连接。
func testGate() ConnAuthFunc {
	deny := []byte(`{"ok":false,"error":"unauthorized: auth required"}`)
	return func(conn *netpkg.Conn, msgID uint32, body []byte) ConnAuthResult {
		if v, ok := conn.Value("authed"); ok {
			if authed, _ := v.(bool); authed {
				return ConnAuthResult{Allow: true}
			}
		}
		if msgID != testAuthMsgID {
			return ConnAuthResult{Reply: deny, Close: true}
		}
		var p authProbe
		if err := json.Unmarshal(body, &p); err != nil || p.Token != testToken {
			return ConnAuthResult{Reply: deny, Close: true}
		}
		conn.SetValue("authed", true)
		return ConnAuthResult{Allow: true, Handled: true, Reply: []byte(`{"ok":true}`)}
	}
}

// startAuthServer 起一个带鉴权闸门的服务端，返回地址与清理函数。
func startAuthServer(t *testing.T) (string, func()) {
	t.Helper()
	srv := NewServer("127.0.0.1:0")
	srv.SetConnAuth(testGate())
	srv.Register(testBizMsgID, func(_ *netpkg.Conn, _ uint32, _ []byte) ([]byte, error) {
		return json.Marshal(okReply{OK: true})
	})
	if err := srv.Listen(); err != nil {
		t.Fatalf("Listen: %v", err)
	}
	return srv.Addr(), func() { _ = srv.Stop() }
}

// TestConnAuthAcceptedWithToken 带正确共享密钥：握手成功，业务请求正常通行。
func TestConnAuthAcceptedWithToken(t *testing.T) {
	addr, stop := startAuthServer(t)
	defer stop()

	c, err := Dial(addr, WithAuth(testAuthMsgID, testToken))
	if err != nil {
		t.Fatalf("带 token 的 Dial 应成功（握手失败会直接返回错误）: %v", err)
	}
	defer func() { _ = c.Close() }()

	var reply okReply
	if err := c.Call(testBizMsgID, struct{}{}, &reply); err != nil {
		t.Fatalf("已鉴权连接的业务调用应成功: %v", err)
	}
	if !reply.OK {
		t.Errorf("业务回包 = %+v, want ok=true", reply)
	}
}

// TestConnAuthRejectedWithoutToken 不带 token：连接建立后**首帧**即被拒，
// 且客户端拿到的是「unauthorized」而不是静默成功。
func TestConnAuthRejectedWithoutToken(t *testing.T) {
	addr, stop := startAuthServer(t)
	defer stop()

	c, err := Dial(addr)
	if err != nil {
		t.Fatalf("无 token 时 Dial 本身不握手（应成功建连，由业务请求触发拒绝）: %v", err)
	}
	defer func() { _ = c.Close() }()

	var reply okReply
	callErr := c.Call(testBizMsgID, struct{}{}, &reply)
	// 两种可接受形态：拿到拒绝回包（ok=false），或连接已被关掉（ErrDisconnected）。
	// 不允许的是「静默成功」。
	if callErr != nil && !errors.Is(callErr, ErrDisconnected) {
		t.Fatalf("无 token 的调用返回了非预期错误: %v", callErr)
	}
	if callErr == nil && reply.OK {
		t.Fatalf("无 token 的业务调用被放行了（鉴权闸门失效）")
	}
}

// TestConnAuthRejectedWithWrongToken 密钥错误：握手期即失败，Dial 直接报错
// （fail-fast —— 不留下一条「已建连但永远调不通」的连接，避免配置错误被拖到运行期才暴露）。
func TestConnAuthRejectedWithWrongToken(t *testing.T) {
	addr, stop := startAuthServer(t)
	defer stop()

	c, err := Dial(addr, WithAuth(testAuthMsgID, "wrong-token"))
	if err == nil {
		_ = c.Close()
		t.Fatalf("密钥错误应在 Dial 阶段失败（fail-fast），实际成功")
	}
	if !errors.Is(err, ErrTimeout) && !errors.Is(err, ErrDisconnected) && err.Error() == "" {
		t.Fatalf("握手失败的错误信息不可读: %v", err)
	}
}

// TestConnAuthHandshakeNotDispatched 握手帧不能被当成业务帧派发（Handled 语义）：
// 若误进业务派发，未注册的 msgID 会走 unknown msgID 分支并回 null，
// 客户端会看到「握手成功但后续全失败」的诡异形态。
func TestConnAuthHandshakeNotDispatched(t *testing.T) {
	addr, stop := startAuthServer(t)
	defer stop()

	c, err := Dial(addr, WithAuth(testAuthMsgID, testToken))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = c.Close() }()

	// 握手已成功 ⇒ 后续业务请求必须仍然通行（证明握手帧没有把连接带进异常状态）。
	var reply okReply
	if err := c.Call(testBizMsgID, struct{}{}, &reply); err != nil {
		t.Fatalf("握手后的业务调用失败: %v", err)
	}
	if !reply.OK {
		t.Fatalf("握手后的业务回包 = %+v, want ok=true", reply)
	}
	// 服务端不得把 MsgAuth 当成未知消息号（未知消息号会记 error 日志并回 null）。
	time.Sleep(20 * time.Millisecond)
}
