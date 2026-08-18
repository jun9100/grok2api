# Grok Build Resin 出口租约方案

状态：进行中。

已完成：出口 IP 只读观测、`egress_ip_records` 持久化、Build IPv4 租约表与原子容量原语、Resin sticky 身份探测及 fail-closed 调度开关。
已完成：账号标记与租约/IP 的只读关联快照、五分钟 IP/账号请求聚合窗口、完整成功响应的推理事件结果聚合。
已完成：跨账号/IP 风险关联标签与管理员只读汇总接口。
尚未完成：基于灰度数据的强制策略与自动隔离；默认不启用。

## 目标

在不为每个账号手工创建大量 Resin 代理节点的前提下，为 Grok Build 账号提供稳定、可审计的出口调度：

- 同一账号复用同一个 Resin sticky 身份和出口租约；
- 为公网 IPv4 设置可配置的并发容量上限；
- 区分账号风控、出口 IP 风控和 Resin 入口故障；
- 仅在 IP 级证据充分时隔离 IP，不能因单个账号异常误杀无关账号；
- 旧 egress、Web、Console 行为保持兼容。

第一阶段仅覆盖 Grok Build。Web/Console 的 SSO 和 Cloudflare clearance 不在本提案范围内。

## 现状与约束

资料中的“单 IP 不超过 3 个账号”“10 分钟约 5 个账号”“使用两小时后冷却”等均是观察到的运行经验，不能当作 Resin 或 Grok 的公开保证。它们只能作为可配置、默认 observe-only 的灰度假设，必须由本地样本验证后才可启用强制拦截。

当前实现已有可复用基础：

- `manager.go` 支持 `{account}` 代理模板、账号身份渲染、`ProxyPool` 和账号连接隔离；
- `egress_nodes` 保存探测到的 IPv4/IPv6、健康度、冷却时间和 `account_capacity`；
- 质量守护支持节点主动探测、被动审计、隔离和恢复；
- Build 账号已有 `build_bot_flag_source`/`bfs` 路由元数据；
- 账号和节点目前直接绑定，没有出口 IP 租约，也没有同 IP 账号计数。

不能只将现有 `accountCapacity` 设置为 `3`。它限制的是节点绑定数，不是公网 IP 的绑定数。一个 Resin 入口可能轮换多个 IP，一个公网 IP 也可能被多个入口复用。

## 核心模型

### 对象边界

| 对象 | 含义 | 可信度 |
| --- | --- | --- |
| Resin 入口 | 节点、代理 URL、ProxyPool | 只是连接入口 |
| 出口 IP | 该账号身份经代理实测的公网 IPv4 | 必须定期验证 |
| 账号租约 | 账号在某出口 IP 的有效绑定 | 系统本地事实 |

调度和容量限制必须以 `exit_ip` 为键，不能以 `egress_node_id` 为键。

### 风控边界

- `account_risk`：SSO 的 `botFlagSource`/BFS、token/权限失效及账号级拒绝。截图材料中 `1` 常与注册环境风险相关，`2` 常与使用期 IP farm 相关；来源和采集时间必须保存，且结论须以实际 SSO 记录为准。
- `ip_farm_risk`：同一 IPv4 上出现多个独立账号的关联账号级证据，例如多账号的 `botFlagSource=2` 或明确的 IP-farm 返回码。
- `ip_rate_risk`：该 IPv4 的 RPM、并发或短时间账户切换过高导致的请求级降级。它可不改变账号 BFS，因此不能从账号标记缺失推导为 IP 正常。
- `model_outcome`：目标模型响应是否含预期的 `thinking_content`/推理链，以及状态码、模型、首字延迟等观测。TPS 只作辅助健康指标，不能单独判定降智。
- `egress_health`：代理连接、IP 探测、延迟、吞吐异常；它不是模型质量或风险状态。
- `unknown`：证据不足时保留观察，不自动禁用账号或 IP。

`account_risk`、`ip_farm_risk`、`ip_rate_risk`、`model_outcome` 和 `egress_health` 必须是独立字段和事件流，禁止用单一“健康/降智”布尔值混合表达。账号级证据只隔离账号；IP 级证据才隔离 IP 租约。单次没有推理链的响应也不足以归因，需保留账号、出口 IP、租约、模型和负载窗口后交叉验证。

## 数据模型

### `egress_ip_records`

新增表，不复用节点上的单一 `exit_ip` 字段：

- `id`、`ip`、`family`、`provider`、`scope`；
- `first_seen_at`、`last_seen_at`、`last_probe_at`；
- `health_status`、`risk_status`、`risk_reason`、`quarantined_until`；
- `source_node_id` 只记录最近来源；
- `(ip, family, scope)` 唯一索引。

### `egress_ip_leases`

- `id`、`ip_record_id`、`account_id`、`account_identity`；
- `state`：`active`、`expired`、`released`、`quarantined`；
- `acquired_at`、`renewed_at`、`expires_at`、`released_at`；
- `last_verified_at`、`last_error`、`release_reason`；
- 每个 `account_id + scope` 最多一个 active lease；
- 同一 IP 的 active lease 数默认最多 `3`。

租约计数和创建必须在一个数据库事务或分布式锁中完成，禁止“先计数、后插入”的竞态实现。

### 风险与结果事件

租约表只解决“当时哪个账号经哪个 IP 出口”，不能证明风险归因。需要追加不可变事件和滚动聚合，至少包含：

- `account_risk_observations`：账号、SSO 来源、`botFlagSource` 值、观察时间、原始状态摘要；
- `ip_usage_windows`：IP、窗口起止、请求数、RPM、并发峰值、独立账号数、独立 sticky 身份数；
- `model_outcome_samples`：账号、租约、IP、模型、`thinking_content` 是否存在、状态码、耗时和采样请求标识；
- `build_egress_outcome_windows`：IP、账号、模型、窗口内成功响应与推理事件计数。

任何原始 SSO 响应、令牌、完整代理 URL 均不得落库或出现在管理 API 中。

### 配置

默认关闭，保持旧行为：

```yaml
egressLease:
  enabled: false
  scope: grok_build
  maxAccountsPerIPv4: 3
  leaseTTL: 30m
  verifyInterval: 5m
  uniqueAccountsWindow: 2h
  maxUniqueAccountsPerWindow: 3
  maxRequestsPerMinute: 0 # 0 表示只观测，不限流
  enforcementMode: observe # observe | enforce
  requireVerifiedIP: true
  failClosed: true
```

`failClosed=true` 时，无法确认公网 IP 的账号不得进入租约池，也不得静默回退为直连。

## 调度流程

### 获取或续期

1. 查 Build 账号的 active lease；未过期且未超过独立 `verifyInterval` 时复用。
2. 没有有效租约时，选择健康的 Resin sticky 节点。
3. 用稳定的 `build_<account_id>` 渲染 `{account}`，经该代理探测公网 IP。
4. 将实测 IP upsert 到 `egress_ip_records`。
5. 在事务中锁定该 IP，确认 active lease 数小于阈值后创建租约。
6. IP 满载、被隔离或探测失败时换入口重试；达到观察或强制限制后返回“无可用出口租约”。

复用前超过 `verifyInterval` 时，必须经同一个渲染后的 sticky URL 再次探测：出口 IP 变化则失效旧租约、记录新 IP 后重新分配；探测失败则按 `failClosed` 拒绝，不能沿用未经验证的旧归因。

### 请求与释放

- 请求沿用租约对应的 Resin 身份；
- 审计记录 `account_id`、`lease_id`、`node_id`、`exit_ip`、模型结果及负载窗口；
- 同一账号跨 IP 时释放旧租约，再对新 IP 重新计数；
- `botFlagSource=1` 等账号级风控仅释放并隔离该账号；不得把 IP 标为异常；
- 单个 `botFlagSource=2` 先记录为关联证据，只有同 IP 多账号相关证据或独立 IP 请求级异常达到阈值时才隔离 IP；
- `ip_rate_risk` 触发时先降低/阻断该 IP 的新请求，不能将它自动等同为所有账号已被账号级标记；
- 账号连接隔离继续使用 `accountIsolatedConnections`，避免跨账号共享上游连接。

## 风控与质量守护

IP 级判定需有最小、可配置且可审计的关联证据，例如“同一 `exit_ip`、指定窗口内、至少 N 个独立账号出现相同 IP 风控标记”。这个示例不是当前默认事实或线上阈值。单账号错误、单次缺失 `thinking_content` 或单次 TPS 异常均不能污染整个 IP。

质量守护保留 TPS、首字延迟、主动探测和被动审计，但 TPS 只用于连接/性能异常的辅助定位。降智判断应优先采样目标模型响应的 `thinking_content`，并以交叉样本确认。状态键扩展为：

```text
scope + egress_node_id + exit_ip
```

没有验证到 IP 时，节点只能是 `egress_health=unknown`。主动探测使用专门的 Build 探测账号，不计入普通账号的 IP 容量。

## API 与管理端

第一阶段只增加可观测性和策略开关：

- 节点页显示实测 IP、active lease 数、IP 风险状态；
- 账号页显示 `lease_id`、`exit_ip`、过期时间和账号风险；
- 质量守护页增加 IP 维度的隔离原因和恢复倒计时；
- 提供手动释放租约、重新探测操作；
- API 绝不返回 Resin 用户名、密码或完整代理 URL。
- `GET /api/admin/v1/build-egress-risk?window=10m&limit=100` 返回出口 IP 的聚合证据与观察标签；该接口不执行隔离。

## 迁移与回滚

1. fork 最新 `main`，创建 `feature/build-egress-ip-lease` 分支。
2. `egressLease.enabled=false` 时零行为变化。
3. 首次启用仅纳入 Build 的 sticky 节点；固定 IP 节点可直接建立记录。
4. 旧 `accountCapacity` 保留为入口级保护；新增 `maxAccountsPerIPv4` 为 IP 级限制。
5. 不批量修改现有账号绑定，首次请求按实测结果渐进建立租约。
6. 回滚关闭开关并停止续租；历史租约只保留用于审计。

## 测试与验收

必须覆盖：

- 10 个并发分配也不能使一个 IP 超过 3 个 active lease；
- 同账号复用租约，IP 变化时正确释放旧租约；
- 账号级风险不影响同 IP 其他账号；
- IP 级风险隔离该 IP 的全部租约；
- 无可验证 IP 时 `failClosed` 生效；
- SQLite WAL、Redis 多实例下没有重复分配；
- 旧模式的 Build/Web/Console egress 测试全部通过。

灰度期间监控每 IP active/窗口账号数分位数、RPM/并发、租约续期率、跨 IP 率、按来源划分的账号/IP 风控率、`thinking_content` 缺失率、403/429、成功率、首字延迟、吞吐和 `failClosed` 拒绝数。所有指标都要保留账号、IP、模型、租约和时间窗口关联，以便区分账号污染和 IP 降级。

验收条件：灰度窗口内的强制容量/窗口策略符合配置；出口 IP 变化可在一个验证周期内纠正；账号级异常不会隔离其他账号；IP 级异常有可复核的多账号或负载证据；`thinking_content` 结果与账号/IP 归因可复现；Build 成功率不低于旧模式基线。

## Fork 前必须验证

1. `{account}` 是否确实产生稳定、可区分的公网 IPv4；
2. 单个 Resin 入口是否能承载足够多的独立 sticky 身份；
3. Resin IP 轮换是否可被探测并及时感知；
4. IP 探测端点与 Grok 实际观察的出口是否一致；
5. `thinking_content` 是否对目标模型和目标请求类型是稳定的降智判据；
6. active 账号数、窗口内独立账号数、RPM 与并发的实际安全边界分别是什么；
7. 同一账号的 SSO `botFlagSource` 能否稳定采集，并可与请求结果交叉验证。

如果第 1 项不成立，单入口不能实现独立 IP 租约；必须保留多个入口，或由 Resin 提供显式 IP lease API。

## 实施顺序

先完成只读出口观测、SSO 账号风险审计、IP 负载窗口和模型结果采样，不改变调度；确认 Resin sticky 身份的 IP 行为及归因后实现租约表和分配；最后仅在灰度数据支持时启用容量、窗口限额和 IP 隔离。自动隔离必须默认关闭，直到具备可复核的关联证据。
