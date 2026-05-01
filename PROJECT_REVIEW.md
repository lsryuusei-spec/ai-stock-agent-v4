# 项目检查报告（AI Stock Agent MVP）

## 1) 当前功能概览

根据 `README.md` 与 CLI，项目目前已经实现一个“多阶段股票研究/候选池管理”MVP，主要包含：

- 基于 LangGraph 的候选池工作流编排（周期复盘、事件驱动刷新等）。
- SQLite 持久化：候选池、审计、归档、复盘、网页研究记录。
- 可插拔数据源与网页研究源：`mock` / `file` / `http`，以及可选的 `tushare`、`akshare`、`alltick`。
- 多市场演示：US / CN / HK。
- CN/HK 股票池的“文件或 akshare”初始股票池构建。
- 网页证据到触发事件（trigger-event）的合成能力。
- 轻量 Dashboard（`serve-gui`）用于可视化查看池子、上下文与研究快照。

## 2) 使用方式（推荐最小路径）

### 2.1 快速上手

1. 初始化 demo 数据
2. 查看可用 universe
3. 执行一次周期复盘工作流
4. 查看池子与上下文输出

示例命令见 `README.md` 的 Quick Start。

### 2.2 常用 CLI 能力（按开发/运营视角分组）

- 初始化与构建
  - `bootstrap-demo`
  - `build-universe`
- 运行工作流
  - `run-mvp`（`periodic_review` / `event_driven_refresh`）
- 结果观察与审计
  - `show-pool`, `show-latest-run`, `show-run-trace`, `show-audit`, `show-history`, `show-postmortem`
- 数据/研究源检查
  - `show-data-sources`, `show-web-sources`, `show-research`, `show-official-status`, `show-evidence`
- 知识库相关
  - `ingest-knowledge*`, `show-knowledge*`, `refresh-knowledge`, `notebooklm-prep`
- 运营与人工干预
  - `manual-override`
  - `serve-gui`

### 2.3 环境与配置重点

- 配置主要在 `config/`：
  - 数据源：`data_sources*.json`
  - 网页研究源：`web_research*.json`
  - 股票池构建：`universe_builder*.json`
- 可选环境变量：
  - `TUSHARE_TOKEN`
  - `ALLTICK_API_KEY`

## 3) 这个智能体当前“判断策略”是什么（重点）

你提到的痛点非常关键：目前最需要被讲清楚的是“它到底如何做决策”。基于代码结构，当前策略可以概括为 **“预筛 + 打分 + 状态机约束 + 证据修正”**。

### 3.1 决策主链路（简化）

1. **候选唤醒**：周期模式下跑全局，事件模式下根据 ticker/event 唤醒局部实体。
2. **挑战者预筛（prescreen）**：先判断是否值得进入深度打分。
3. **深度评分（scorecard）**：对通过预筛的标的计算综合评分。
4. **状态迁移校验**：即使分数变化，也必须满足允许的 bucket/thesis 迁移规则。
5. **入池/降级/归档**：输出到 core/secondary/shadow/archive 等状态。

### 3.2 预筛（Prescreen）看什么

预筛主要在 `prescreen_challenger` 中完成，核心规则包括：

- 信息分不低于阈值（`prescreen_min_info_score`）
- 流动性分不低于阈值（`prescreen_min_liquidity_score`）
- 风险惩罚不高于阈值（`prescreen_max_risk_penalty`）
- 与主题因子的重合度达到最小要求（`prescreen_min_factor_overlap`）
- 若市场上下文是 `data_blocked` / `degraded`，会触发 defer 或置信度折减
- 若 `risk_off` 且实体风险惩罚过高，会直接拒绝

输出不是只有“过/不过”，还会区分：
- `deep_score`：进入深度评分
- `shadow_watch`：先观察
- `defer`：数据阻断等场景下暂缓

### 3.3 评分（Scorecard）看什么

深度评分在 `compute_scorecard`，核心是“基础分 + 上下文修正 + 证据修正 + 知识修正 - 惩罚”：

- **基础维度**：质量、行业位置、宏观匹配、估值、催化、风险惩罚
- **市场上下文修正**：根据市场 regime（如 `risk_off`）和 breadth 指标增减宏观匹配分
- **主题重合加分**：标的因子暴露与当前主题切片重合越高，加分越多
- **证据可信度修正**：证据置信度低会扣分，高且有官方证据会加分
- **知识库覆盖修正**：宏观信号、拥挤度信号、原则信号、topic diff 风险标记会进一步修正
- **时效惩罚**：数据/证据陈旧会增加惩罚

这意味着它不是“单一分数模型”，而是 **多来源加权 + 风险闸门** 的混合策略。

### 3.4 状态机约束：防止“乱跳档”

系统显式定义了允许的 bucket/thesis 迁移集合（例如 secondary→core 可以，但不是任意状态都能互跳）。

意义是：
- 避免一次偶然数据波动导致状态剧烈抖动
- 让“升级/降级/退役”有明确流程边界

### 3.5 当前策略仍不透明的原因

虽然规则在代码里，但“可解释输出”还不够集中，主要缺：

1. **统一策略文档**：规则散落在 prescreen/scoring/workflow。
2. **每次决策的贡献分解**：缺少“这次+2/-4来自哪条规则”的标准化日志视图。
3. **参数版本治理**：阈值/权重变更后，缺少内建对比报告。

### 3.6 建议优先补的透明化功能（建议直接做）

- 增加 `show-decision --entity-id ... --run-id ...`：输出“预筛结果 + 打分拆解 + 状态迁移合法性 + 最终动作”。
- 在数据库新增 decision_explain 表，存储每条规则命中和分值贡献。
- 将评分参数与阈值配置化并版本化（如 `policy_v2026_05`），支持回放。
- Dashboard 增加“Why this decision?” 面板，减少读代码成本。

## 4) 后续开发建议（按优先级）

### P0：可用性与可靠性

1. **建立“配置有效性校验器”**
   - 在运行前自动检查 provider 配置、token、fallback 链是否完整，减少运行时才暴露错误。
2. **完善工作流可观测性**
   - 为 `run-mvp` 增加统一 run_id 追踪、阶段耗时和失败点统计，并在 dashboard 聚合显示。
3. **标准化错误分级**
   - 将 provider 错误分为“可降级/不可降级”，并输出结构化诊断，方便自动告警。

### P1：研究与策略能力

4. **因子与评分体系版本化**
   - 把评分参数、阈值、权重做显式版本管理，支持回放对比（A/B score profile）。
5. **事件驱动增强**
   - 丰富 `event_driven_refresh` 的 event taxonomy（业绩、监管、产业链、舆情突变）。
6. **知识库到策略联动**
   - 将知识条目映射到可执行“规则片段”（如行业禁入、估值红线、信号置信度修正）。

### P2：工程化与团队协作

7. **测试分层**
   - 单元测试（纯逻辑）/集成测试（SQLite + workflow）/回归快照测试（固定输入输出）分层。
8. **数据契约文档化**
   - 明确每类 provider 的输入输出 schema，减少接入新数据源的对接成本。
9. **部署与运维模板**
   - 增加 Docker Compose 或轻量部署模板，方便多环境（dev/staging/prod）一致化运行。

## 5) 风险与注意事项

- 外部供应商（tushare/akshare/alltick）可用性和限频策略会影响稳定性，需重点监控 fallback 命中率。
- 当前 MVP 适合研究辅助与流程验证，不应直接视作自动化实盘交易系统。
- 多市场支持已具雏形，但实盘级“数据质量、时效、合规、风控”仍需单独建设。

## 6) 关于你问的 Codex 历史状态继承

结论：**通常不能只靠“换电脑登录”自动继承本地运行态**，但可以继承“代码与会话记录”的一部分，取决于你怎么工作。

- 能稳定继承的：
  - Git 仓库中的代码与提交历史（push 到远端后）。
  - 若平台有云端会话历史，则可看到历史对话，但不等于恢复本地进程状态。
- 不能直接继承的：
  - 当前终端中的临时进程、内存态、未保存文件、未提交变更。
  - 本地容器的瞬时数据库/缓存（除非你持久化并同步）。

**建议做法：**
1. 频繁提交（小步提交）。
2. 将关键开发上下文写入仓库文档（如 `PROJECT_REVIEW.md`、`DEV_NOTES.md`）。
3. 对 SQLite/样本数据做可重建脚本，避免依赖“容器瞬时状态”。
4. 每次切换设备前：`git add/commit/push`，并记录下一步 TODO。
