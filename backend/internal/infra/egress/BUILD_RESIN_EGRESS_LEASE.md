# Grok Build Resin 出口租约方案

状态：进行中。

已完成：出口 IP 只读观测、`egress_ip_records` 持久化、Build IPv4 租约表与原子容量原语。
尚未完成：Resin sticky 身份探测接入、请求调度开关、IP 风险隔离与管理端展示。

## 目标

在不为每个账号手工创建大量 Resin 代理节点的前提下，为 Grok Build 账号提供稳定、可审计的出口调度：

- 同一账号复用同一个 Resin sticky 身份和出口租约；
- 一个实际公网 IPv4 最多承载 3 个 Build 账号，默认值可配置；
- 区分账号风控、出口 IP 风控和 Resin 入口故障；
- IP 异常自动隔离和恢复，不误杀无关账号；
- 旧 egress、Web、Console 行为保持兼容。

第一阶段仅覆盖 Grok Build。Web/Console 的 SSO 和 Cloudflare clearance 不在本提案范围内。

## 现状与约束

资料中的“单 IP 不超过 3 个账号”可作为策略假设，但不是 Resin 或 Grok 的公开保证，必须通过灰度数据验证。

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

- `account_risk`：`bfs`、`botFlagSource=1`、token/权限失效及账号级拒绝；
- `ip_risk`：`botFlagSource=2`、`eapi_ip_bot_farm`、`no_token_farm` 或同 IP 多账号相同异常；
- `egress_health`：代理连接、IP 探测、延迟、吞吐异常；
- `unknown`：证据不足，不能自动禁用账号。

账号级证据只隔离账号；IP 级证据才隔离 IP 租约。质量守护的 TPS 异常默认只是 egress 健康信号，不能直接认定账号或 IP 被风控。

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

### 配置

默认关闭，保持旧行为：

```yaml
egressLease:
  enabled: false
  scope: grok_build
  maxAccountsPerIPv4: 3
  leaseTTL: 30m
  verifyInterval: 5m
  requireVerifiedIP: true
  failClosed: true
```

`failClosed=true` 时，无法确认公网 IP 的账号不得进入租约池，也不得静默回退为直连。

## 调度流程

### 获取或续期

1. 查 Build 账号的 active lease；未过期且最近验证成功则复用。
2. 没有有效租约时，选择健康的 Resin sticky 节点。
3. 用稳定的 `build_<account_id>` 渲染 `{account}`，经该代理探测公网 IP。
4. 将实测 IP upsert 到 `egress_ip_records`。
5. 在事务中锁定该 IP，确认 active lease 数小于阈值后创建租约。
6. IP 满载、被隔离或探测失败时换入口重试；达到限制后返回“无可用出口租约”。

### 请求与释放

- 请求沿用租约对应的 Resin 身份；
- 审计记录 `account_id`、`lease_id`、`node_id`、`exit_ip` 和风险分类；
- 同一账号跨 IP 时释放旧租约，再对新 IP 重新计数；
- 账号级风控仅释放该账号；IP 级风控将同 IP 所有租约置为 `quarantined`，相关账号重新参与分配；
- 账号连接隔离继续使用 `accountIsolatedConnections`，避免跨账号共享上游连接。

## 风控与质量守护

IP 级判定需有最小证据，例如“同一 `exit_ip`、10 分钟窗口内、至少 2 个独立账号出现相同 IP 风控标记”。单账号错误不能污染整个 IP。

质量守护保留 TPS、首字延迟、主动探测和被动审计，但状态键扩展为：

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

灰度期间监控每 IP 账号数分位数、租约续期率、跨 IP 率、账号/IP 风控率、403/429、成功率、首字延迟、吞吐和 `failClosed` 拒绝数。

验收条件：灰度窗口内没有超过 3 账号/IP 的有效租约；IP 异常可在一个探测周期内隔离；Build 成功率不低于旧模式基线。

## Fork 前必须验证

1. `{account}` 是否确实产生稳定、可区分的公网 IPv4；
2. 单个 Resin 入口是否能承载足够多的独立 sticky 身份；
3. Resin IP 轮换是否可被探测并及时感知；
4. IP 探测端点与 Grok 实际观察的出口是否一致；
5. “3 个账号/IP”是否需要按更短的时间窗口收紧。

如果第 1 项不成立，单入口不能实现独立 IP 租约；必须保留多个入口，或由 Resin 提供显式 IP lease API。

## 实施顺序

先完成只读出口观测和 IP/账号风险审计，不改变调度；确认 Resin sticky 身份的 IP 行为后实现租约表和分配；最后再启用 `maxAccountsPerIPv4=3` 强约束与自动隔离。
