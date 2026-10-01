# AI Workflow Engine（Python）

可本地验证的 AI 工作流编排与执行服务：客户端提交由多个相互依赖的步骤组成的工作流定义并触发执行，服务跟踪并对外呈现每个步骤与整次运行的最终状态。全程不访问外部网络、不需要真实凭据或付费服务。

## 快速开始（本地验证）

```bash
uv sync                 # 安装依赖
uv run pytest tests/    # 运行测试（日志中会打印每个用例的输入与判定依据）
uv run pytest tests/test_resume.py  # 只跑失败续跑专项（续跑成功/不重复/幂等/并发/超限）
uv run pytest tests/test_success_clears_error.py  # 成功步骤不残留历史失败描述专项
uv run uvicorn main:app --port 8000   # 启动 HTTP 服务
```

测试全程为本地确定性动作（`flaky` / `sleep` / `record` 等）与进程内调用，**不访问网络**。

### HTTP 接口示例

```bash
# 提交运行（X-Owner 头标识调用者身份）
curl -s -X POST localhost:8000/runs -H 'Content-Type: application/json' \
  -H 'X-Owner: alice' -d '{
    "idempotency_key": "req-1",
    "definition": {
      "workflow_id": "demo",
      "steps": [
        {"id": "extract", "action": "echo", "params": {"value": "raw"}},
        {"id": "summarize", "action": "echo", "depends_on": ["extract"]}
      ]
    }}'
# => {"run_id": "..."}

curl -s localhost:8000/runs/<run_id> -H 'X-Owner: alice'      # 查询状态
curl -s -X POST localhost:8000/runs/<run_id>/cancel -H 'X-Owner: alice'  # 取消

# 失败续跑：运行处于 FAILED/TIMED_OUT 终态后，由原提交者发起
curl -s -X POST localhost:8000/runs/<run_id>/resume \
  -H 'Content-Type: application/json' -H 'X-Owner: alice' \
  -d '{"idempotency_key": "resume-1"}'
# => {"run_id": "...", "resume_id": "..."}
```

## 工作流定义方式

```json
{
  "workflow_id": "demo",
  "steps": [
    {
      "id": "step-a",                 // 必填，运行内唯一
      "action": "echo",               // 必填，本地动作名
      "params": {"value": 1},         // 可选，动作参数
      "depends_on": ["step-b"],       // 可选，上游步骤 id 列表
      "timeout_seconds": 2.0,         // 可选，步骤级超时
      "max_retries": 2,               // 可选，可重试失败的重试次数
      "idempotency_key": "order-1"    // 可选，逻辑副作用幂等键
    }
  ]
}
```

内置本地动作（`app/actions.py`，可扩展）：`echo`、`fail`（kind=retryable/fatal）、
`flaky`（前 N 次可重试失败）、`sleep`、`record` / `record_then_fail`（副作用账本）。

## 状态流转

步骤：`PENDING → RUNNING → SUCCEEDED / FAILED / TIMED_OUT`；
上游未成功时下游置为 `SKIPPED`（与 `FAILED` 明确可区分，且 `attempts=0`）；
运行被取消/超时时未终态步骤置为 `CANCELLED`。

运行：`PENDING → RUNNING → SUCCEEDED / FAILED / CANCELLED / TIMED_OUT`，
终态时 `reason` 字段记录明确原因（如“运行超时：总耗时超过上限…”），不会返回看似成功的状态。

**成功步骤不残留历史失败描述**：只要一个步骤最终是 `SUCCEEDED`，对外查询到的 `error`
一律为 `null`——无论是同一次运行里先失败、重试之后才成功，还是上一轮失败、这一轮续跑
补跑成功（含续跑轮次内先失败再重试成功），都以最终成功为准，之前任何一次尝试留下的
失败/超时描述都会被清掉。真正没跑成的步骤（不可重试失败、重试耗尽、超时、被上游拦住
`SKIPPED`、被取消）的未完成说明照旧保留，不受这条规则影响。

## 失败续跑（resume）

一次运行进入 `FAILED` / `TIMED_OUT` 终态后，**原提交者**可以发起续跑，把没跑成的部分补上，
而不必从头再来。接口：`POST /runs/{run_id}/resume`，可选 `idempotency_key`，返回
`{run_id, resume_id}`；查询同一个 run 看到的始终是最新状态（响应中新增 `resume_count`）。

续跑语义：

- **成功步骤沿用结果**：上一轮已 `SUCCEEDED` 的步骤不再真正执行，直接沿用上一次输出；
  其 `attempts` 不增加，也不会因续跑产生第二次逻辑副作用。
- **未成功步骤重新尝试**：上一轮 `FAILED` / `TIMED_OUT`，以及因上游没成功而被搁置的
  `SKIPPED`（含取消轮次中的 `CANCELLED`）步骤，全部回到 `PENDING` 重新调度，并重新获得
  完整的重试预算；上游一旦补齐成功，之前被搁置的下游会正常往下走。
- **副作用不重复**：带 `idempotency_key` 的步骤，其副作用账本与首次输出持久化在运行记录上、
  跨轮次复用；即使该步骤上一轮“效果已生效但响应失败”，续跑也命中缓存而不再执行动作。
- **状态干净**：续跑成功后看到的是这一轮最新、自洽的状态，已成功步骤上不残留本轮或
  上一轮的失败描述（见“状态流转”中的成功步骤规则），`reason` 也会被新结果覆盖。
- **每轮预算独立**：每次续跑重新受 `run_timeout_seconds`（单次总耗时）约束，重新计时；
  步骤总数沿用原定义（定义不可变，故上限天然照旧），并发度、重试上限照旧生效。

续跑的状态流转：终态 `FAILED/TIMED_OUT → RUNNING → SUCCEEDED/FAILED/CANCELLED/TIMED_OUT`，
即同一 run 在终态后可重新进入 `RUNNING`；`SUCCEEDED` 与 `CANCELLED` 终态不可续跑。

防重复与并发：

- 相同 `(原提交者, 续跑幂等标识)` 重复提交只真正生效一次，返回**一致的 `resume_id`**。
- 同时到来的多个续跑请求只允许一个成功，其余得到明确的 `resume_conflict`（HTTP 409）。
- 初始执行尚未结束时发起续跑，得到 `resume_not_allowed`（409），与“并发续跑冲突”可区分。
- 续跑轮次同样可被取消；取消后线程被 join 回收、不残留后台任务，且取消终态不可再续跑。
- 单个运行的续跑次数受 `max_resumes_per_run` 约束，超限返回 `resume_limit_exceeded`
  （HTTP 413）并在 message 中说明上限，不会被悄悄当成成功，也不会无限续下去。

## 错误分类

所有对外错误归一化为 `{error: {code, message}}`，底层异常绝不原样透出：

| code | HTTP | 含义 |
|---|---|---|
| `invalid_definition` | 422 | 定义结构不合法 |
| `cyclic_dependency` | 422 | 依赖构成环 |
| `missing_dependency` | 422 | 依赖不存在的步骤 |
| `duplicate_step_id` | 422 | 步骤标识重复 |
| `resource_limit_exceeded` | 413 | 步骤数等超过配置上限 |
| `run_not_found` | 404 | 运行不存在 |
| `run_forbidden` | 403 | 运行存在但不属于调用者（与 404 可区分） |
| `run_conflict` | 409 | 运行已终态/取消中，操作冲突 |
| `resume_not_allowed` | 409 | 运行未结束（初始执行中），或终态不是失败/超时（成功/取消不可续跑） |
| `resume_conflict` | 409 | 已有续跑在推进中，并发续跑只允许一次生效 |
| `resume_limit_exceeded` | 413 | 单个运行的续跑次数超过配置上限 |

步骤级错误：`step_timeout`（超时）、`retryable_step_error`（可重试，按指数退避重试，
次数受 `max_retries` 与全局上限约束）、`fatal_step_error`（不可重试，立即失败）。

## 限制项配置

`WorkflowService` / `Executor` 构造参数（`app/main.py` 中装配）：

- `max_steps_per_run`（默认 64）：单次运行步骤总数上限，超限拒绝且不产生运行记录；
- `max_concurrency`（默认 4）：无依赖步骤的并行度上限；
- `run_timeout_seconds`（默认 60）：单次运行总耗时上限，超时按策略终止并给出原因；
- `max_retries_cap`（默认 5）：步骤重试次数的全局上限；
- `backoff_base_seconds`（默认 0.05）：重试退避基数（指数退避，封顶 5s）；
- `max_resumes_per_run`（默认 3，`WorkflowService` 构造参数）：单个运行允许的续跑次数上限，
  超限以 `resume_limit_exceeded`（413）明确拒绝。步骤总数 / 单次总耗时 / 并发度 / 重试上限
  在每次续跑中照旧生效（单次总耗时每轮重新计时）。

## 关键语义

- **静态校验前置**：解析与 DAG 校验全部在创建运行记录之前完成，拒绝时不留半初始化记录。
- **幂等提交**：相同 `(owner, idempotency_key)` 重复提交返回首次的 `run_id`，不产生第二次执行。
- **幂等副作用**：带 `idempotency_key` 的步骤，效果账本命中后直接返回缓存结果，
  同一逻辑副作用不会生效第二次。
- **取消/超时回收**：取消或超时后执行线程被 join 回收，未开始的任务被取消，
  不留后台推进的任务或半更新状态；并发取消只有一次生效，其余得到 `run_conflict`。
- **失败续跑**：仅失败/超时终态可由原提交者续跑；成功步骤沿用旧输出不重跑、副作用不重复，
  失败/超时/被搁置的步骤重试，下游在上游补齐后自动放行；重复续跑（相同提交者+幂等标识）
  只生效一次并返回同一 `resume_id`，并发续跑只允许一个成功（其余 `resume_conflict`），
  续跑次数超 `max_resumes_per_run` 明确拒绝。
- **归属隔离**：运行记录只能被提交者（`X-Owner`）访问，越权返回 403，与不存在（404）可区分。
