# master 分片改造 · 步骤文档

> 目标：让 master 从**单点**变成**按 key 分片的 N 个实例**，不需要主备、不需要 leader 选举。
> 依据：`clover-doc/server/concepts/cluster.md` §Master 分片与节点目录、§跨节点数据可见性；`ai-skill/reference/conventions.md` §7。
>
> **执行策略（已定）**：**先把 S1–S5 全部功能实现完，最后统一跑实机验证**，不在每步之间起服。
> 因此每一步的完成标志是「`go build ./... && go vet ./...` 通过」，实跑只在 §七 做一次。

## 一、验收标准（全部满足才算完成）

1. 起 2 个 master（`master_shard.index=0/1`、`master_shard.total=2`）+ 1 个 game，game 能把 **uid 稳定路由到唯一分片**（同一 uid 每次都落到同一分片）。**2026-09-15 实跑通过**（3 分片，见 §八）。
2. 定位表（uid → nodeID）**只写在属主分片**，任一 uid 的注册/查询/删除都路由到同一 master。**2026-09-15 实跑通过**：逐分片 `MsgPlayerLookup` 反查，6/6 uid 只被 `Xxhash64(uid)%total` 所属那台命中。
   > 原验收口径写的是「两个 master 日志各只应出现归属自己的那些 uid」——**该口径不可执行**：master 的定位注册/查询/删除路径不打印任何 uid。详见 §8.2。
3. master 崩溃/重启**不丢**定位表与排行（从 etcd / Redis 恢复）。**未实测**：定位表本身在内存（§二），重启后靠 game 重新注册；排行 / session token 走 Redis 备份 —— 这条要有 Redis 持久化 + 重启用例才能验，本轮没做。
4. 不配分片（`total=1` 或缺省）时，行为与改造前**完全一致**（向后兼容，单机部署零感知）。**未单独跑用例**，但单机 `all` 配置本轮前后一直在跑、行为一致。
5. `go build ./... && go vet ./...` 通过。**通过**。

## 二、master 现在的 4 类数据（`internal/domain/master/state/state.go`；默认全在内存，session token 已支持切 Redis 后端）

| 数据 | 字段 | 读/写特征 | 目标落点 |
|---|---|---|---|
| 节点表 / 健康 | `nodes` / `byType` / `byTag` | 写少、要全局一致 | **etcd**（lease + watch） |
| 玩家定位 | `players` / `nodePlayers` | **读极多**（每次跨节点投递都查）、写中等 | **按 uid 哈希分片**到 N 个 master |
| 排行榜 | `rankMgr` | 写多、可最终一致 | **按榜名哈希分片** + Redis 备份 |
| session token | `tokenBackend` | 低频（每人约 12h 一次 refresh） | **Redis**（已支持该后端） |

关键频率证据：`MsgPlayerLookup`（定义于 `internal/domain/master/state/wire.go:40`，发送点 `internal/domain/master/client/player_client.go:35`）在每次按玩家跨节点投递前调用（`internal/transport/event/crossnode.go:494,611`）；`MsgNodesByType` 每 5s 轮询（`crossnode.go:333` 间隔常量、`:340` 循环、`:390` 调用；注入节点目录后改读本地缓存、不再出网）；token 见 `sessiontoken/store.go:38-44` 的 12h 节流。

## 三、分片规则（唯一契约，两端必须一致）

| 项 | 规则 |
|---|---|
| 分片键 | **uid（玩家 ID 字符串）**；排行榜用**榜名** |
| 归属计算 | `shard = util.Xxhash64(uid) % master_shard.total`（`pkg/shared/util` 已有） |
| 分片发现 | etcd：`clover/services/master/<index>` → JSON `{"addr":"...","total":N}` |
| 兜底 | 未配 etcd / 分片列表为空 → 回落 `master_addr` 静态地址（= 单分片行为） |

## 四、步骤清单

| # | 步骤 | 产出物 | 验收 | 依赖 |
|---|---|---|---|---|
| **S1 ✅** | **分片身份 + etcd 注册** | `master.ShardConfig`（`internal/domain/master/config.go`）；`internal/app/master_shard_discovery.go` 的 `registerMasterShard`；`runMaster` 接线 | `etcdctl get --prefix clover/services/master/` 能看到 N 个分片；`total=1` 时行为不变 | 无 |
| **S2 ✅** | game 侧分片解析 + 按 uid 路由 | `masterShardResolver`（同文件，GetPrefix + WatchPrefix + `pick`）；`client.ShardedClient`；`PlayerClient.call` 按 uid 取分片；`CrossNodeEventBus.UseShardedMaster`；bootstrap 接线 | 同一 uid 恒落同一分片；起 2 master 时定位注册 / 查询 / 删除都落属主 | S1 |
| S3 ✅ | 定位表切到分片（行为闭环） | `crossnode` 按 uid 取分片客户端 | 端到端：player 注册→查询→删除全走属主分片 | S2 |
| S4 ✅ | 节点表 / 健康交 etcd | S4a 写入侧：`internal/app/node_registry.go`；S4b 读路径：`node_directory.go` + `crossnode.SetNodeSource`，见下方「S4 落点」 | 摘掉一个 game 节点，租约到期即从存活集合消失 | S1 |
| S5 ✅ | 排行榜分片 + session token 分片 | 见下方「S5 落点」 | 同一榜 / 同一玩家恒落同一分片；`BackupAll` 覆盖全部分片 | S2 |

### S4 落点（节点表 / 健康交 etcd）

| 环节 | 现在 | 改成 |
|---|---|---|
| game 注册自身 | `registerService(roleLogic)` 只写裸地址 | 追加写 `clover/nodes/<nodeID>` = `{type, tags, admin}`，lease 续租 |
| game 查节点 | `AuthorityClient.NodesByType/NodesByTag` → master TCP | watch `clover/nodes/` 前缀，本地缓存；查表不出网 |
| 跨节点投递存活判断 | `crossnode.liveNodes` 每 5s 拉一次 master | 同一份 etcd 本地缓存（去掉 5s 轮询） |
| master 节点表 / 健康 | `State.nodes` + `failover` 心跳探测 | **退役**：存活 = lease 未过期（etcd 自动摘除），master 不再做心跳判定 |
| drain 摘流 | `MsgRemoveNode` | 撤销自己的 etcd 注册（lease 回收即摘流） |

影响面：`internal/app/discovery.go`、`bootstrap.go`、`internal/transport/event/crossnode.go`、`internal/domain/master/{state,failover,server}`、`internal/app/drain.go`。**改动面最大的就是这一步**，落地前需先确认 drain / gateway upstream 不受影响。

**进度**：S4a（写入侧）已完成 —— `internal/app/node_registry.go` 把本节点 `{type,tags,admin}` 以租约注册到 `clover/nodes/<nodeID>`，未配 etcd 时空操作，**不改变任何现有读路径**。
S4b（读路径切换）已完成（见 §七状态表）：① 节点**类型**对齐为 `type` 与 `state.Node.Type` 同取值；② `drain` 摘流仍走 `MsgRemoveNode`，etcd 侧由租约到期兜底（与 gateway upstream 无冲突）。

### S5 落点（排行榜分片 + token）

| 环节 | 改成 |
|---|---|
| 排行榜 | `RankClient` 复用 `ShardedClient`，分片键 = **榜名**（同一榜恒落同一分片；跨榜查询不聚合） |
| session token | 多分片部署**推荐** `master_session_token.backend=redis`：`Client` 已按 playerID 路由 session（与定位同键），故 memory 后端在分片下同样正确；推荐 redis 的理由仅为「进程重启不丢 token」（见 §七 S5 与实现口径） |
| 备选 | 或让 token 也按 playerID 走 `ShardedClient` 路由（与定位一致）；本期先选 Redis（改动更小） |

## 五、S1 的最小形状（本轮实现）

- 配置：`master_shard: { index: 0, total: 1 }`（零值即单分片，等同改造前）。
- master 启动：`total>=1` 且配了 etcd → 注册 `clover/services/master/<index>`，值 `{"addr":"<listen_addr>","total":N}`；lease 续租，进程退出自动摘除。
- 未配 etcd → 只打印一条日志，跳过注册（不阻断启动）。
- 不改任何现有消息号与 handler 行为。

## 六、风险与不做的事

| 风险 | 处置 |
|---|---|
| 分片总数变更（扩容）导致 key 重映射 | 本期**不支持热扩容**：改 `total` 需停服迁移；文档写明 |
| game 侧发现失败 | 回落静态 `master_addr`；列表为空时打 Warn |
| 排行/token 与定位跨分片不一致 | 分片键只选「uid / 榜名」，不跨键；不做分布式事务 |

**不做**：leader 选举、Raft、跨 master 数据复制、跨分片查询聚合。

## 七、进度与验证

> **策略（已定）**：**先把功能全部做完（S1–S5），最后统一跑实机**；不在步骤之间起服。
> 阶段完成标志 = `cd clover-server-engine && go build ./... && go vet ./...` 通过。

| 步骤 | 状态 | 说明 |
|---|---|---|
| S1 分片注册 | ✅ | master 启动按 `master_shard.index/total` 注册 `clover/services/master/<index>`（JSON 含 addr + total）；未配 etcd 只告警不阻断。 |
| S2 按 uid 路由 | ✅ | game 解析分片表（watch 自愈），定位的注册 / 查询 / 删除按 `Xxhash64(uid)%total` 路由；**未配 etcd 或 total=1 时走原单连接路径，行为与改造前一致**。 |
| S3 定位表落分片 | ✅ | 已随 S2 覆盖，无需单独改动。 |
| S4a 节点目录写入 | ✅ | `internal/app/node_registry.go`：租约注册 `clover/nodes/<nodeID>` = `{type,tags,admin}`（`type` 与 `state.Node.Type` 同取值）；注销并入 `g.etcdCancel`。 |
| S4b 节点读路径切换 | ✅ | `internal/app/node_directory.go`（watch 本地缓存）+ `CrossNodeEventBus.SetNodeSource`（存活集合改读目录，去掉 5s 轮询）+ `Game.NodesByTag` 优先读目录。**master 节点表保留为未配 etcd 的兜底**（不做破坏性退役）。drain 摘流仍走 `MsgRemoveNode`，etcd 侧由租约到期兜底。 |
| S5 排行榜 / token 分片 | ✅ | `Client` 统一按 key 路由：定位=uid、排行榜=榜名、session=playerID；`BackupAll` / `RestoreAll` 广播至全部分片；memory token 后端在分片下同样正确（按 playerID 路由），多分片仍推荐 redis（重启不丢）。 |
| 静态验证 | ✅ | 截至 S5 + S4a：`go build ./... && go vet ./...` 通过。 |

**端到端验证步骤**（引擎本身没有 `main`，需在业务工程里跑；见 ai-skill `scaffold/new-project.md`）：

```bash
# 1. 起依赖
clover-server-tools/windows-env/core/env.exe start

# 2. 起两个 master（server.yaml 里 master_listen_addr 不同、master_shard 分别 index 0 / 1、total 2，且都配 etcd.endpoints）
# 3. 确认注册
clover-server-tools/windows-env/etcd/etcdctl.exe --endpoints=127.0.0.1:2379 get --prefix clover/services/master/
# 期望：clover/services/master/0 = {"addr":"...:8021","total":2}
#       clover/services/master/1 = {"addr":"...:8022","total":2}

# 4. 起一个 game（master_shard.total=2），日志应出现：
#    discovery: master shards updated: total=2 addrs=map[0:... 1:...]
#    crossnode: master sharding enabled for node ... (player location routed by uid)
# 5. 玩家上下线后，**逐分片反查定位**（见下方"验收口径修正"）
```

### 7.1 端到端实跑结果（2026-09-15，载体 `clover-mmo-1`）✅

3 个分片实跑：`shard-0`(8021, index 0/3) / `shard-1`(8022, index 1/3) / `all-shard`(8023, index 2/3，同进程承载网关 + 逻辑 + 账号 + 日志)。
配置：`clover-mmo-1/server/configs/{shard-0,shard-1,all-shard}.yaml`；完整记录见 `clover-mmo-1/docs/验证报告.md` §B-7。

| 验收点 | 实测 |
|---|---|
| 分片注册 | `etcdctl get --prefix clover/services/master/` → 3 条 `{"addr":"127.0.0.1:802x","total":3}` |
| game 侧发现 | `discovery: master shards updated: total=3 addrs=map[0:…8021 1:…8022 2:…8023]` |
| 路由启用 | `master client: shard routing enabled` + `crossnode: master sharding enabled for node 127.0.0.1:8011` |
| **按 uid 归属** | 6 个真实客户端上线，逐个分片发 `MsgPlayerLookup(62)` 反查：**每个 uid 只被 `Xxhash64(uid)%3` 所属那一台命中，6/6 通过**（探针 `.codebuddy/doc-audit/tools/master-probe`） |

#### 实跑查出的缺陷（已修）

**装配顺序错误 ⇒ 分片路由与 etcd 场景路由双双静默失效**：

- 现象：配了 `etcd.endpoints` + `master_shard.total=3`，etcd 里分片也注册成功，但 game 侧日志是
  `WARN app: master_shard.total=3 but etcd is not configured; player location stays on static master`，
  外加 `WARN app: scene route uses in-memory backend (no etcd)`。
- 根因：`RunGame` 里 etcd 客户端在 `setupNATSBackends` **之后**才创建，而后者正是读 `g.etcdCli`
  装配「场景路由后端」与「master 分片解析器」的地方 —— 读到的一直是 nil。
- 危害：**服务发现照常工作**（logic/log/auth 注册都正常），只有这两项静默降级，静态验证（`go build` / `go vet` / 单测）全部通过。
- 修法：把 etcd 客户端创建移到 `setupNATSBackends` 之前（`internal/app/bootstrap.go`），
  并在「配了 etcd 却拿到 nil」时改为 **Error** 级告警、点名装配顺序（防止再被写反）。

#### 验收口径修正（原口径不可观测）

原写「玩家上下线，两个 master 日志各只应出现归属自己的那些 uid」——**master 在定位注册 / 查询 / 删除路径上不打印任何 uid**（`internal/domain/master/server/server.go` 的 `MsgPlayerRegister/Remove/Lookup` handler 与 `state.go` 全无 logger），该口径无法执行。
现口径：**逐分片反查** `MsgPlayerLookup(62)`，断言「found=true 的分片 == `Xxhash64(uid)%total`」且**有且仅有一台**命中。

**本轮未验证项**：
- `total=1` 回归（静态地址路径不变）未单独跑 —— 单机 `all` 配置（`configs/all/server.yaml`，无 etcd / 无 `master_shard`）本轮前后一直在跑，行为与改造前一致，但这是**旁证不是用例**。
- §一 验收 3（master 重启后定位表 / 排行不丢）未测 —— 定位表在内存、重启后靠 game 重新注册，要验它得另做 Redis 持久化 + 重启用例。
