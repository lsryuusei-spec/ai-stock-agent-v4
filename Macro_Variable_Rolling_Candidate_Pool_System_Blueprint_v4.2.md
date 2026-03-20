# Macro Variable Rolling Candidate Pool System Blueprint v4.2

## 1. 文档定位
本文档是“宏观变量驱动的滚动研究候选池系统”的 V4.2 总蓝图，是后续智能体编排、状态建模、评分引擎、数据管线与审计追踪的唯一系统级事实来源。

它不是单节点提示词，不是代码实现说明，也不是交易指令模板。它定义的是一套可长期运行、可追溯、可回滚、可扩展的研究候选池状态机。

V4.2 在 V4.1 的业务逻辑基础上，进一步补齐以下工程级空白：
- 确定性状态迁移合同
- 可执行评分公式合同
- 事件幂等、断点恢复与冲突合并
- 数据源优先级、时效门槛与降级策略
- 人工干预边界与审计规则
- Universe 治理与实体映射
- 系统效果评估闭环

## 2. 系统目标与边界

### 2.1 系统目标
本系统的目标不是直接给出买卖指令，而是围绕宏观变量维护一个有记忆、有淘汰机制、有研究优先级排序能力的滚动研究候选池，用于指导投研资源分配。

### 2.2 系统输出
系统可以输出：
- 候选池结构与成员变更
- 每个标的的状态档案与最新 thesis 摘要
- 留池、升降级、出池、观察、重入建议
- 触发事件记录与处理链路
- 评分结果与评分解释
- 历史版本、审计记录、误判归因与策略评估

### 2.3 系统不做的事
系统默认不做以下事情，除非在外层策略中另行定义：
- 自动下单
- 自动仓位管理
- 自动止盈止损执行
- 以 LLM 主观判断替代财务计算或价格计算
- 以单轮新闻情绪替代长期 thesis 验证

## 3. V4.2 核心工程原则

### 3.1 LLM 与 Code 的强边界
- LLM 负责：定性归因、文本压缩、因果梳理、假设生成、反证整理
- Code 负责：数学计算、衰减计算、阈值判断、预算裁切、状态迁移、幂等校验、落库合并

### 3.2 Frozen Quant Packet 原则
每次进入 N2、N7、N8 前，必须由代码层生成只读 `FrozenQuantPacket`。LLM 只能读取，不得自行心算、拼接、累加、比较绝对数值。

### 3.3 StateDelta 原则
LLM 不直接改写长期状态。LLM 只能输出增量变化建议，底层由代码执行 `StateDelta` merge。

### 3.4 可恢复执行原则
所有运行必须支持：
- 幂等重试
- 中断恢复
- 冲突检测
- 版本化回放

### 3.5 审计优先原则
凡是影响分数、状态、入池、出池、覆盖历史结论的操作，必须可追溯到输入、版本、操作者与计算路径。

## 4. 运行模式
- `Mode A: Initial Build`：首次从 Universe 建立约 20 支候选池。
- `Mode B: Periodic Review`：按固定周期对全池或子池复盘。
- `Mode C: Event-Driven Refresh`：由 Trigger Triage 命中阈值后，只对受影响范围局部重估。
- `Mode D: Recovery / Replay`：用于失败运行恢复、历史重放、评分策略升级后的回灌。

## 5. Universe 治理

### 5.1 universe_state
`universe_state` 是独立一等对象，至少包含：
- `universe_id`
- `market`
- `effective_date`
- `eligible_entities`
- `excluded_entities`
- `entity_mapping_version`
- `universe_rules_version`

### 5.2 Universe 纳入规则
必须显式定义：
- 市场范围与证券类型
- 最低流动性门槛
- 最低信息可得性门槛
- 财报可解析性要求
- 停牌、退市预警、重大重组的处理规则

### 5.3 实体映射规则
必须有统一 `entity_id`，解决：
- 同股不同市
- ADR / 本地股映射
- 历史改名
- 并购后主体续接
- 多 ticker 对应单研究主体

## 6. 顶层数据对象

### 6.1 run_state（单次运行快照）
包含：
- `run_id`
- `run_mode`
- `run_status`：`created | running | partial_failed | merged | aborted | replayed`
- `market`
- `macro_theme`
- `trigger_event_ids`
- `wake_scope`
- `input_snapshot_hash`
- `policy_version_set`
- `incumbent_review_set`
- `challenger_set`
- `decision_output`
- `idempotency_key`
- `parent_run_id`

### 6.2 research_pool_state（长期候选池全局状态）
包含：
- `pool_id`
- `market`
- `last_updated_at`
- `current_pool_members`
- `shadow_watch_members`
- `archived_members`
- `pool_capacity_policy`
- `active_policy_versions`

### 6.3 company_state_record（公司长期状态档案）
包含：
- `entity_id`
- `ticker`
- `company_name`
- `current_bucket`
- `current_route`
- `thesis_status`
- `current_quality_score`
- `trajectory_score`
- `retention_priority_score`
- `recent_thesis_summaries`
- `historical_tags`
- `last_primary_evidence_date`
- `last_confirmed_date`
- `freshness_window_days`
- `staleness_penalty`
- `staleness_level`
- `manual_override_flags`
- `active_factor_exposures`
- `score_version`

### 6.4 治理与控制对象
- `WeightCalibrationPolicy`
- `FactorRegistry`
- `FrozenQuantPacket`
- `TriggerEventRecord`
- `StateDelta`
- `AuditTrailRecord`
- `ManualOverrideRecord`
- `DataSourceHealthRecord`
- `ExecutionRecoveryRecord`

## 7. 池结构与正式 Bucket 定义

### 7.1 Bucket 分类
- `core_tracking`：核心跟踪，6-8 支
- `secondary_candidates`：次级候选，8-10 支
- `high_beta_watch`：高弹性观察，4-6 支
- `shadow_watch`：影子观察，等待补证、等待时机、平局保留、重入前观察
- `archived`：出池归档

### 7.2 Shadow Watch 正式治理
`shadow_watch` 必须有明确规则：
- 进入条件：Arena 平局、挑战者证据未满但赔率突出、重入前观察、事件后暂缓定论
- 最大停留期：如 2-3 个 review 周期
- 再晋级条件：证据补齐、催化剂增强、估值赔率仍有效
- 自动淘汰条件：冷却期结束后无新增证据，或 thesis 失效

## 8. 确定性状态机合同

### 8.1 thesis_status 枚举
- `forming`
- `validated`
- `accelerating`
- `fragile`
- `broken`
- `retired`

### 8.2 current_route 枚举
- `incumbent_review`
- `challenger_scan`
- `arena_competition`
- `shadow_observation`
- `archive_monitoring`
- `manual_review`

### 8.3 bucket 迁移规则
仅允许以下迁移：
- `secondary_candidates -> core_tracking`
- `high_beta_watch -> secondary_candidates`
- `shadow_watch -> secondary_candidates`
- `core_tracking -> secondary_candidates`
- `secondary_candidates -> high_beta_watch`
- `any_active_bucket -> archived`
- `archived -> shadow_watch`

禁止跨级跃迁，除非存在 `manual_override` 且必须留痕。

### 8.4 thesis_status 迁移规则
- `forming -> validated | fragile`
- `validated -> accelerating | fragile | broken`
- `accelerating -> validated | fragile | broken`
- `fragile -> validated | broken | retired`
- `broken -> retired | shadow_watch_reentry_candidate`
- `retired` 不能自动回升，只能经 reentry 新建追踪链

### 8.5 reentry 规则
重入不是简单恢复旧状态，必须：
- 创建新 `review_chain_id`
- 重算 `breakthrough_bonus_score`
- 旧历史仅可作为背景材料，不得直接继承有效性
- 若旧 thesis 已 `broken`，必须明确写出新 thesis 与旧 thesis 的差异点

## 9. Trigger Triage 与事件治理

### 9.1 TriggerEventRecord
必须包含：
- `event_id`
- `event_type`
- `event_time`
- `source_type`
- `source_ref`
- `event_deduplication_key`
- `wake_scope`
- `impacted_entities`
- `trigger_confidence`
- `cooldown_group`
- `parent_event_id`

### 9.2 触发参数
必须配置：
- `hard_trigger_thresholds`
- `soft_trigger_router_confidence_threshold`
- `trigger_cooldown_window`
- `event_deduplication_key`
- `max_daily_wakeups`
- `wake_scope`

### 9.3 幂等与冷却
- 同一 `event_deduplication_key` 在冷却窗口内不得重复唤醒同范围任务
- 同一实体在同一日内若收到多事件，应先做事件合并，再决定单次或多次局部重跑
- 所有唤醒必须生成 `idempotency_key`

### 9.4 事件冲突合并
当多个事件同时命中同一实体：
- 先按 `wake_scope` 聚合
- 再按 `event_type` 分组
- 若分数影响方向一致，可合并为单次刷新
- 若方向冲突，进入 `manual_review` 或 `dual_hypothesis` 分支

## 10. Snapshot 生命周期与历史折叠

### 10.1 主状态树保留
长期主状态树保留：
- 基础身份信息
- 当前 bucket
- 当前 route
- thesis_status
- 三层评分
- 最新变更摘要
- 结构化时间字段

### 10.2 富文本保留
仅保留最近 2 次完整定性 review 的富文本内容。

### 10.3 老旧信息折叠
更早历史必须折叠为：
- `historical_tags`
- `evidence_counters`
- `milestone_markers`

### 10.4 外部归档召回边界
RAG 召回必须受硬约束：
- 时间窗口不超过过去 18 个月
- 单次最多召回 5 篇核心历史日志
- 召回日志必须标记原始时间与版本

## 11. Frozen Quant Packet 合同

### 11.1 标准结构
```json
{
  "as_of_date": "YYYY-MM-DD",
  "data_freshness_hours": 6,
  "raw_metrics": {},
  "derived_metrics": {},
  "peer_percentiles": {},
  "valuation_snapshot": {},
  "tradability_snapshot": {},
  "source_manifest": [],
  "packet_hash": "..."
}
```

### 11.2 数据合同要求
- 所有字段必须有 schema
- 缺失字段必须显式标记为 `null` 或 `missing_reason`
- 每个指标必须记录来源与时间戳
- `packet_hash` 必须参与后续审计

### 11.3 数据新鲜度门槛
必须定义：
- `max_data_age_intraday`
- `max_data_age_eod`
- 财报字段的最大可接受滞后
- stale packet 是否允许进入 N2/N7/N8

当超过门槛时，系统只能：
- 降级为定性观察
- 推迟该实体评分
- 标记为 `data_blocked`

## 12. 数据源优先级与降级策略

### 12.1 Data Contracts & SLA
每类数据需定义：
- 主数据源
- 备份数据源
- 最低字段完整率
- 最大延迟容忍
- 缺字段默认处理
- 故障时的降级路径

### 12.2 降级原则
- 价格缺失：不得生成估值比较结论
- 财报关键字段缺失：不得进入完整深度评分
- 新闻源异常：允许跳过软触发，但不得伪造否定结论
- 行业对比缺失：保留实体评分，但降低置信度乘数

## 13. Factor Compilation & Accounting Pipeline

### 13.1 Factor Library 隔离
必须区分：
- `global_factor_library`
- `market_pack`
- `sector_pack`

不得将市场特有规则写入全局因子库。

### 13.2 FactorRegistry 必备字段
- `factor_id`
- `factor_name`
- `factor_type`
- `scope`
- `evidence_requirements`
- `budget_cap`
- `multiplier_rule`
- `cluster_id`
- `exclusion_rules`
- `shadow_mode`
- `shadow_cycles_required`
- `promotion_criteria`
- `retirement_criteria`

### 13.3 5 关审查编译器
所有新增因子必须通过：
1. 分类合法性检查
2. 证据充分性检查
3. 预算裁切检查
4. 冲突与重复计分检查
5. 沙盒与影子试运行检查

## 14. Weight Calibration Policy

### 14.1 分层预算上限
示例：
- `fundamental_cap = 40`
- `industry_cap = 25`
- `macro_cap = 15`
- `valuation_cap = 10`
- `catalyst_cap = 10`

### 14.2 作用分型
每个因子必须属于以下之一：
- `score_additive`
- `multiplier_only`
- `veto_only`
- `trigger_only`

### 14.3 乘数流
证据置信度与时效衰减以乘数进入：
- `quant_verified = 1.2x`
- `filing_based = 1.1x`
- `management_guidance = 0.95x`
- `news_sentiment = 0.8x`

所有乘数必须由代码层计算，LLM 不得主观发明。

## 15. 三层评分合同

### 15.1 current_quality_score
定义：标的在当前时点的静态质量与主题相关性。

输入包括但不限于：
- 基本面质量
- 行业结构位势
- 与宏观主题的一致性
- 资产可交易性
- 风险暴露结构

### 15.2 trajectory_score
定义：相较上期 thesis、证据与催化剂方向的变化强度。

输入包括：
- 证据增量
- 证据破坏
- 催化剂强化或削弱
- 市场验证程度变化
- 风险事件变化

### 15.3 retention_priority_score
定义：用于留池、升降级、Arena 决策的最终排序分。

必须由代码层按明确公式计算，例如：
```text
retention_priority_score =
normalized(
  quality_component
  + trajectory_component
  + valuation_component
  + freshness_component
  + breakthrough_component
  - risk_penalty_component
)
```

### 15.4 评分合同要求
必须明确：
- 每个子项输入字段
- 每项取值范围
- 缺失值处理
- 归一化方法
- veto 优先级
- score_version

## 16. breakthrough_bonus_score 合同

### 16.1 适用对象
仅适用于：
- `new_challenger`
- `reentry_challenger`

### 16.2 启动条件
必须同时满足：
- 有至少一项核心新证据
- 估值或赔率出现明显边际改善
- 未触发 hard veto

### 16.3 计算约束
- 仅在 Arena 中生效
- 上限不得超过 Arena 总比较权重的 15%
- 不可跨轮保留
- 下一轮自动重算或失效
- 若挑战者基础质量低于最低门槛，则 bonus 失效

### 16.4 抗偏置规则
若 defender 的历史证据已过保质期，必须剥夺其“文本完整度护城河”优势，避免旧档案天然压制新入场者。

## 17. Defender-Challenger Arena

### 17.1 defender_set
从现有池中选出 thesis 动摇、证据老化、赔率显著下降或重复度过高的 3-5 支边缘标的。

### 17.2 challenger_set
从 Universe 中通过量化筛选、主题筛选、资讯事件、重入监测得到 3-8 支挑战者。

### 17.3 强制比较顺序
1. `Hard Veto / Trigger Disqualifier`
2. `Retention Priority Score`
3. `Valuation & Odds`
4. `Evidence Freshness`
5. `Breakthrough Bonus`
6. `Thesis Narration`

### 17.4 Tie-breaker
- Challenger 若显著改善池内多样性，优先
- Defender 若近期证据刚更新且 thesis 未破，优先
- 若极度焦灼，则 Defender 留位，Challenger 进入 `shadow_watch`

## 18. 智能体节点架构（N0-N13）

### 18.1 前置层
- `Triggers / Quant Pipelines`
- `N0 Pool Initializer`

### 18.2 全局环境层
- `N1 Theme Decomposition & Scenario Tree`
- `N2 Market Context Loader`

### 18.3 存量评估层
- `N3 Incumbent Pool Health Check`
- `N4 Defender Selection`

### 18.4 增量扫描层
- `N5 External Challenger Scan`
- `N6 Challenger Prescreen`
- `N7 Dual Routing + Deep Scoring`

### 18.5 决策更新层
- `N8 Defender-Challenger Arena`
- `N9 Entry/Exit Decision Engine`
- `N10 State Delta Writer + Code Merge`
- `N11 Pool Reassembler`
- `N12 Archive & Version Control`

### 18.6 反馈层
- `N13 Dynamic Post-Mortem Loop`

## 19. 确定性执行合同

### 19.1 幂等键
以下行为必须有 `idempotency_key`：
- 触发唤醒
- 评分计算
- 状态 merge
- 归档写入

### 19.2 中断恢复
系统必须支持：
- 从上一个成功节点恢复
- 跳过已完成且 hash 未变的节点
- 对失败的 N10 merge 做安全重试

### 19.3 部分失败策略
若局部实体失败：
- 允许 run 进入 `partial_failed`
- 成功实体可先 merge
- 失败实体必须写入 `ExecutionRecoveryRecord`

### 19.4 冲突合并策略
若两个 run 同时修改同一 `company_state_record`：
- 先比较 `input_snapshot_hash`
- 若基础快照一致，则按事件优先级合并
- 若基础快照不一致，则拒绝自动 merge，进入冲突队列

## 20. Manual Override 合同

### 20.1 可人工干预的范围
允许人工覆盖：
- bucket 最终归属
- thesis_status 最终定级
- 是否入池 / 出池
- 某次触发事件是否忽略

### 20.2 不可人工改写的范围
不得直接人工改写：
- 历史审计记录
- 原始 FrozenQuantPacket
- 已落库的输入 hash
- 代码计算出的原始基础分

### 20.3 ManualOverrideRecord
必须记录：
- `override_id`
- `target_object_id`
- `override_field`
- `old_value`
- `new_value`
- `reason`
- `operator`
- `effective_until`
- `created_at`

人工覆盖必须有时效，不得默认为永久。

## 21. Audit Trail 合同
每次关键决策必须包含：
- `decision_id`
- `review_id`
- `input_snapshot_hash`
- `state_delta_hash`
- `policy_version_set`
- `who_computed_what`
- `llm_output_hash`
- `merged_by`
- `merged_at`

## 22. Schema 与存储

### 22.1 Schema
所有输入输出必须通过强类型 schema 定义，可选实现：
- Pydantic
- Zod

### 22.2 存储分层
- Transactional DB：PostgreSQL 或 SQLite
- Object Store：原始快照、评分包、日志
- Vector DB：归档后的长文本、历史分析

### 22.3 版本字段
至少追踪：
- `schema_version`
- `score_version`
- `factor_registry_version`
- `policy_version`
- `prompt_version`
- `merge_engine_version`

## 23. 评估与反馈闭环

### 23.1 系统级评估指标
必须持续追踪：
- `entry_hit_rate`
- `challenger_promotion_success_rate`
- `defender_retention_quality`
- `false_positive_trigger_rate`
- `false_negative_miss_rate`
- `score_drift_by_version`
- `shadow_to_production_factor_win_rate`

### 23.2 N13 输出要求
每轮复盘至少产出：
- 错判案例
- 迟判案例
- 因子膨胀案例
- 触发误唤醒案例
- 状态迁移异常案例

## 24. 智能体落地建议
建议采用 LangGraph 或同类具备状态图、条件边、恢复能力的编排器。

推荐按三层搭建：
- `Control Layer`：Trigger、RunState、Recovery、Audit、Merge
- `Analysis Layer`：N1-N9 的 LLM/规则混合节点
- `Persistence Layer`：State Store、Vector Store、Version Store

## 25. 最小可实施版本（MVP）建议
第一阶段不必一次上全量能力，建议最小闭环为：
1. Universe + Pool State + Company State 三类核心表
2. Trigger Triage 最小版
3. FrozenQuantPacket 最小版
4. N3/N4/N5/N7/N8/N9/N10 主链路
5. AuditTrail + ManualOverride + Recovery 最小版

## 26. 附录：实施纪律
- 任何新因子先进入 `shadow_mode`
- 任何新评分公式必须升 `score_version`
- 任何新状态字段必须给出迁移脚本
- 任何手工修正必须进审计
- 任何 LLM 节点输出必须 schema 校验后才能入下一节点

---

本蓝图为后续智能体搭建、数据库设计、节点拆分、提示词约束、评分引擎编码与回测评估的统一依据。
