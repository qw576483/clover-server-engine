package proto

import (
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// 本文件把「消息号数值层」的不变量固化成测试。
//
// 为什么必须由单测守数值：跨端比对工具只比双端**载体字段集合**，不比消息号**数值**，
// 实测因此漏过两类问题：
//   - 白名单键过时（写 EPlayerFullSync，真名已是 EPlayerFullSyncNotify）导致比对静默失效；
//   - 两端同名不同值 / 重复号位 / 业务号侵占引擎段，只能靠人眼比对。
//
// 测试数据来自**解析源码本身**（而不是硬编码一份副本），因此新增消息号会自动被覆盖，
// 也不会出现「测试和实现一起改所以永远通过」的假绿。

var (
	// goConstRe 匹配本包内的 `const E<Name> uint32 = <值>`。
	goConstRe = regexp.MustCompile(`(?m)^\s*const\s+(E?\w+)\s+uint32\s*=\s*(0x[0-9A-Fa-f]+|\d+)\s*$`)
	// csConstRe 匹配客户端 EMsg.cs 内的 `public const uint <Name> = <值>;`。
	csConstRe = regexp.MustCompile(`public\s+const\s+uint\s+(\w+)\s*=\s*(0x[0-9A-Fa-f]+|\d+)`)
)

// clientCandidates 给出服务端常量名在客户端 EMsg.cs 中可能的写法，按优先级排列。
//
// 服务端用带语义的前缀（EMsgXxx / EPushXxx），客户端 EMsg 类内直接写短名：
//
//	EMsgLogin           -> Login            （去掉 EMsg）
//	EMsgError           -> Error            （去掉 EMsg，注意不是 "rror"）
//	EPushPlayerFullSync -> PushPlayerFullSync（只去掉 E）
//	InternalMsgMax      -> InternalMsgMax   （非消息号，原样）
func clientCandidates(goName string) []string {
	return []string{
		goName,                              // 原样（InternalMsgMax 这类）
		strings.TrimPrefix(goName, "EMsg"),  // EMsgLogin -> Login
		strings.TrimPrefix(goName, "EPush"), // 兜底
		strings.TrimPrefix(goName, "E"),     // EPushPlayerFullSync -> PushPlayerFullSync
	}
}

// parseGoMsgIDs 解析本包全部 .go（非 _test.go）里的 uint32 常量。
func parseGoMsgIDs(t *testing.T) map[string]uint32 {
	t.Helper()
	return parseGoMsgIDsFromDir(t, ".")
}

// parseGoMsgIDsFromDir 解析 dir 下全部 .go（非 _test.go）里的 uint32 常量。
// 仅匹配字面量定义（`= <数字>`）；跨包再导出（`= pproto.X`）不参与匹配，避免重复计数。
func parseGoMsgIDsFromDir(t *testing.T, dir string) map[string]uint32 {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("读取包目录 %s 失败: %v", dir, err)
	}
	out := map[string]uint32{}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		b, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatalf("读取 %s/%s 失败: %v", dir, e.Name(), err)
		}
		for _, m := range goConstRe.FindAllStringSubmatch(string(b), -1) {
			out[m[1]] = parseNum(t, m[2])
		}
	}
	if len(out) == 0 {
		t.Fatalf("%s 未解析到任何 uint32 消息号常量，正则可能已失效（测试本身失去意义）", dir)
	}
	return out
}

// internalProtoDir 定位引擎内部协议包（internal/shared/proto，含 game↔master 的 6001–6004）。
// 找不到时返回空串（引擎被单独拆分时跳过）。
func internalProtoDir() string {
	p := filepath.Join("..", "..", "..", "internal", "shared", "proto")
	if _, err := os.Stat(p); err != nil {
		return ""
	}
	return p
}

// TestInternalEngineMsgIDs 覆盖「只在服务端存在的引擎内部号」。
//
// 背景（缺陷）：TestClientMsgIDsMatchServer 只扫描 pkg 包；internal/shared/proto 的
// EMasterRoom*（6001–6004，game↔master，客户端 EMsg.cs 本就没有对应常量）成为测试盲区——
// 号位漂移、与对外号段碰撞都不会有测试拦截。这里把 internal 侧号位纳入同一套不变量：
// 落引擎段、不与 pkg 号位碰撞、且与文档约定的 6001–6004 一致。客户端不需要这些号。
func TestInternalEngineMsgIDs(t *testing.T) {
	dir := internalProtoDir()
	if dir == "" {
		t.Skip("未找到 internal/shared/proto（引擎被单独拆分时跳过）")
	}
	pkgIDs := parseGoMsgIDs(t)
	internalIDs := parseGoMsgIDsFromDir(t, dir)

	// 反向索引（数值 → 名字），用于「internal 号位与 pkg 对外号位碰撞」检测。
	pkgByID := make(map[uint32]string, len(pkgIDs))
	for name, id := range pkgIDs {
		pkgByID[id] = name
	}
	for name, id := range internalIDs {
		if id == 0 || id > InternalMsgMax {
			t.Errorf("internal %s = %d：超出引擎保留段 (0, %d]", name, id, InternalMsgMax)
		}
		if prev, dup := pkgByID[id]; dup {
			t.Errorf("号位碰撞：internal/%s 与 pkg/%s 都是 %d", name, prev, id)
		}
	}

	// 号位漂移拦截：这 4 条 game↔master 协议按文档（internal/shared/proto/msg.go 头注释）固定 6001–6004。
	want := map[string]uint32{
		"EMasterRoomRegister":      6001,
		"EMasterRoomUnregister":    6002,
		"EMasterRoomFind":          6003,
		"EMasterRoomTakeoverClaim": 6004,
	}
	for name, id := range want {
		got, ok := internalIDs[name]
		if !ok {
			t.Errorf("internal/shared/proto 缺少 %s（期望 %d）", name, id)
			continue
		}
		if got != id {
			t.Errorf("%s = %d，期望 %d（号位漂移会打断 game↔master 协议）", name, got, id)
		}
	}
}

// clientEMsgPath 定位客户端 EMsg.cs；找不到时返回空串（引擎被单独检出时跳过交叉校验）。
func clientEMsgPath() string {
	p := filepath.Join("..", "..", "..", "..", "clover-client-unity-engine", "Runtime", "Network", "EMsg.cs")
	if _, err := os.Stat(p); err != nil {
		return ""
	}
	return p
}

func parseNum(t *testing.T, s string) uint32 {
	t.Helper()
	v, err := strconv.ParseUint(s, 0, 32)
	if err != nil {
		t.Fatalf("解析数值 %q 失败: %v", s, err)
	}
	return uint32(v)
}

// TestEngineMsgIDInRange 校验引擎消息号都落在引擎保留段内，且号位不重复。
func TestEngineMsgIDInRange(t *testing.T) {
	ids := parseGoMsgIDs(t)

	seen := map[uint32]string{}
	for name, id := range ids {
		if name == "EMsgError" {
			// 通用错误回包是「不是成功回包」的标记位，有意跳出所有区间。
			if id != 0xFFFFFFFF {
				t.Errorf("EMsgError 应为 0xFFFFFFFF，实际 %#x", id)
			}
			continue
		}
		if id == 1 {
			t.Errorf("%s = 1：号位 1 是作废保留的注册位（注册已移到账号服 HTTP），不得复用", name)
		}
		if id == 0 || id > InternalMsgMax {
			t.Errorf("%s = %d：超出引擎保留段 (0, %d]；业务号必须 >= %d",
				name, id, InternalMsgMax, InternalMsgMax+1)
		}
		if prev, dup := seen[id]; dup {
			t.Errorf("消息号重复：%s 与 %s 都是 %d", prev, name, id)
		}
		seen[id] = name
	}
}

// TestFrameConstants 校验客户端帧边界常量（双端逐字节一致的前提）。
func TestFrameConstants(t *testing.T) {
	if RequestIDLen != 4 {
		t.Errorf("RequestIDLen = %d，应为 4", RequestIDLen)
	}
	if MsgIDLen != 4 {
		t.Errorf("MsgIDLen = %d，应为 4", MsgIDLen)
	}
	if ClientFrameHeaderLen != 8 {
		t.Errorf("ClientFrameHeaderLen = %d，应为 8（requestID + msgID）", ClientFrameHeaderLen)
	}
	if InternalMsgMax != 10000 {
		t.Errorf("InternalMsgMax = %d，应为 10000（改动即破坏双端号段契约）", InternalMsgMax)
	}
}

// TestPushIDsStartAt4001 校验推送号从 4001 起连续编号（引擎推送段约定）。
func TestPushIDsStartAt4001(t *testing.T) {
	ids := parseGoMsgIDs(t)

	push := map[uint32]string{}
	for name, id := range ids {
		if strings.HasPrefix(name, "EPush") {
			push[id] = name
		}
	}
	if len(push) == 0 {
		t.Fatal("未解析到任何 EPush* 推送号")
	}
	for i := 4001; i <= 4001+len(push)-1; i++ {
		if _, ok := push[uint32(i)]; !ok {
			t.Errorf("推送号 %d 空缺：推送段必须从 4001 起连续编号", i)
		}
	}
}

// TestClientMsgIDsMatchServer 交叉校验客户端 EMsg.cs 与服务端消息号数值。
//
// 客户端可能不存在（引擎被单独检出）——此时跳过而不是失败。
func TestClientMsgIDsMatchServer(t *testing.T) {
	path := clientEMsgPath()
	if path == "" {
		t.Skip("未找到客户端 EMsg.cs（clover-client-unity-engine 不在同级目录），跳过双端数值交叉校验")
	}

	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("读取 %s 失败: %v", path, err)
	}

	cs := map[string]uint32{}
	for _, m := range csConstRe.FindAllStringSubmatch(string(b), -1) {
		cs[m[1]] = parseNum(t, m[2])
	}
	if len(cs) == 0 {
		t.Fatalf("%s 未解析到任何消息号常量，正则可能已失效", path)
	}

	matched := map[string]bool{}
	for goName, srvID := range parseGoMsgIDs(t) {
		var csName string
		var cliID uint32
		var ok bool
		for _, cand := range clientCandidates(goName) {
			if cliID, ok = cs[cand]; ok {
				csName = cand
				break
			}
		}
		if !ok {
			t.Errorf("客户端 EMsg.cs 缺少 %s（服务端 %d）——客户端将无法识别该消息号", goName, srvID)
			continue
		}
		matched[csName] = true
		if cliID != srvID {
			t.Errorf("%s 双端数值不一致：服务端 %d，客户端 %d", goName, srvID, cliID)
		}
	}
	for csName, cliID := range cs {
		if !matched[csName] {
			t.Errorf("客户端 EMsg.cs 多出 %s = %d，服务端无对应常量（号位无归属）", csName, cliID)
		}
	}
}
