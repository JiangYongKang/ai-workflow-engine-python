# AI Workflow Engine（Python）

可本地验证的 AI 工作流编排与执行服务：客户端提交由多个相互依赖的步骤组成的工作流定义并触发执行，服务跟踪并对外呈现每个步骤与整次运行的最终状态。全程不访问外部网络、不需要真实凭据或付费服务。

## 快速开始（本地验证）

```bash
uv sync                 # 安装依赖
uv run pytest tests/    # 运行测试（日志中会打印每个用例的输入与判定依据）
uv run uvicorn main:app --port 8000   # 启动 HTTP 服务
```

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

步骤级错误：`step_timeout`（超时）、`retryable_step_error`（可重试，按指数退避重试，
次数受 `max_retries` 与全局上限约束）、`fatal_step_error`（不可重试，立即失败）。

## 限制项配置

`WorkflowService` / `Executor` 构造参数（`app/main.py` 中装配）：

- `max_steps_per_run`（默认 64）：单次运行步骤总数上限，超限拒绝且不产生运行记录；
- `max_concurrency`（默认 4）：无依赖步骤的并行度上限；
- `run_timeout_seconds`（默认 60）：单次运行总耗时上限，超时按策略终止并给出原因；
- `max_retries_cap`（默认 5）：步骤重试次数的全局上限；
- `backoff_base_seconds`（默认 0.05）：重试退避基数（指数退避，封顶 5s）。

## 关键语义

- **静态校验前置**：解析与 DAG 校验全部在创建运行记录之前完成，拒绝时不留半初始化记录。
- **幂等提交**：相同 `(owner, idempotency_key)` 重复提交返回首次的 `run_id`，不产生第二次执行。
- **幂等副作用**：带 `idempotency_key` 的步骤，效果账本命中后直接返回缓存结果，
  同一逻辑副作用不会生效第二次。
- **取消/超时回收**：取消或超时后执行线程被 join 回收，未开始的任务被取消，
  不留后台推进的任务或半更新状态；并发取消只有一次生效，其余得到 `run_conflict`。
- **归属隔离**：运行记录只能被提交者（`X-Owner`）访问，越权返回 403，与不存在（404）可区分。
