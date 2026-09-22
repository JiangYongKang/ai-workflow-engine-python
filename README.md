# AI Workflow Engine (Python)

一个可完全本地验证的 AI 工作流编排与执行服务。客户端提交由多个相互依赖的步骤组成的工作流定义并触发执行，服务跟踪并对外呈现每个步骤与整次运行的最终状态。**不访问外部网络、不需要真实凭据或付费服务**——所有"模型/工具调用"由本地注册的处理器模拟。

## 快速开始

```bash
uv sync                 # 安装依赖
uv run pytest tests -q -s   # 运行测试（-s 可看到每个用例的输入与判定依据）
uv run uvicorn main:app --port 8000   # 启动服务
```

## 工作流定义

通过 `POST /runs` 提交，请求头 `X-Owner` 标识提交者：

```json
{
  "idempotency_key": "my-key-1",
  "wait": false,
  "workflow": {
    "workflow_id": "demo",
    "steps": [
      {"step_id": "extract", "action": "echo", "params": {"value": "raw"}},
      {"step_id": "summarize", "action": "slow", "depends_on": ["extract"],
       "params": {"duration": 0.5}, "timeout_seconds": 5, "max_retries": 2},
      {"step_id": "notify", "action": "effect", "depends_on": ["summarize"],
       "params": {"effect_key": "notify-user-1"}}
    ]
  }
}
```

- `step_id`：步骤标识，必须唯一。
- `action`：本地注册的处理器名（内置 `echo` / `fail` / `slow` / `effect`）。
- `depends_on`：上游步骤列表，必须构成**有向无环图**。
- `timeout_seconds` / `max_retries`：步骤级超时与重试上限（受全局配置钳制）。
- `wait: true` 时同步等待运行结束后返回，便于本地验证。

## 接口

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/runs` | 提交并触发执行（幂等） |
| GET | `/runs/{run_id}` | 查询运行与全部步骤状态 |
| POST | `/runs/{run_id}/cancel` | 请求取消运行 |

错误响应统一为 `{"error": {"code": ..., "message": ...}}`。

## 状态流转

**运行（Run）**：`PENDING → RUNNING → SUCCEEDED / FAILED / CANCELLED / TIMED_OUT`

**步骤（Step）**：`PENDING → RUNNING → SUCCEEDED / FAILED / TIMED_OUT`，或未执行即终止的 `SKIPPED / CANCELLED`

- 下游步骤只在**全部上游成功**后启动。
- 上游失败/超时/取消时，下游标记为 `SKIPPED`（错误码 `UPSTREAM_FAILED`），与 `FAILED` 可区分，绝不静默丢弃或标记成功。
- 运行被取消或超时后，协调线程会等待全部在途步骤退出才收尾，不残留后台任务或半更新状态。

## 错误分类

| 错误码 | 含义 | HTTP |
|---|---|---|
| `INVALID_DEFINITION` | 定义结构不合法 / 未知动作 | 400 |
| `DUPLICATE_STEP_ID` | 步骤标识重复 | 400 |
| `MISSING_DEPENDENCY` | 依赖的步骤不存在 | 400 |
| `CYCLE_DETECTED` | 依赖构成环 | 400 |
| `STEP_LIMIT_EXCEEDED` | 步骤数超上限 | 400 |
| `RUN_NOT_FOUND` | 运行不存在 | 404 |
| `FORBIDDEN` | 运行属于其他提交者 | 403 |
| `RETRYABLE_FAILURE` / `NON_RETRYABLE_FAILURE` | 步骤内可重试 / 不可重试失败 | — |
| `RETRIES_EXHAUSTED` | 可重试失败重试耗尽 | — |
| `STEP_TIMEOUT` / `RUN_TIMEOUT` | 步骤 / 运行超时 | — |
| `UPSTREAM_FAILED` | 上游未成功导致跳过 | — |
| `RUN_CANCELLED` | 运行被取消 | — |
| `INTERNAL` | 未预期内部错误（底层异常不透出） | 500 |

定义类错误在**执行开始前**拒绝，不留下任何运行记录，同一幂等键可立即重新提交合法定义。处理器抛出的任何底层异常都被归一化为上述错误类型，不原样透出到接口。

## 故障处理与幂等

- **重试**：仅 `RETRYABLE_FAILURE` 触发重试，次数取 `min(步骤 max_retries, 全局 max_retries)`，指数退避 `retry_backoff_base * 2^(attempt-1)`。
- **副作用幂等**：处理器可通过 `EffectLedger.apply_once(effect_key, fn)` 保证同一逻辑副作用在一次运行内只实际生效一次，重试不会产生第二次效果。
- **提交幂等**：同一 `(owner, idempotency_key)` 重复提交返回首次的 `run_id`，不产生第二次执行。
- **访问控制**：运行记录仅提交者可读写；越权返回 `403/FORBIDDEN`，与 `404/RUN_NOT_FOUND` 可区分。

## 限制项配置（`EngineConfig`）

| 配置 | 默认 | 说明 |
|---|---|---|
| `max_concurrency` | 4 | 并行执行步骤数上限 |
| `max_steps_per_run` | 64 | 单次运行步骤总数上限，超限拒绝 |
| `max_run_seconds` | 60 | 单次运行总耗时上限，超限终止为 `TIMED_OUT` |
| `default_step_timeout` | 10 | 步骤默认超时（秒） |
| `max_retries` | 3 | 可重试失败的重试次数上限 |
| `retry_backoff_base` | 0.05 | 指数退避基数（秒） |

## 本地验证

```bash
uv run pytest tests -q -s
```

测试覆盖：循环依赖等静态校验（`test_validation.py`）、失败重试与错误分类（`test_execution.py`）、并行执行与并发取消（`test_concurrency.py`）、越权访问与幂等提交（`test_api.py`）。每个用例在日志中打印**输入与判定依据**，全部在进程内运行，不访问网络。

## 代码结构

```
app/
  config.py      # 限制项配置
  errors.py      # 错误分类（ErrorCode / WorkflowError）
  models.py      # 领域模型与状态枚举
  validation.py  # 定义解析与 DAG 静态校验
  providers.py   # 本地动作处理器注册表 + 幂等效果账本
  store.py       # 运行记录存储（幂等提交、访问控制）
  executor.py    # 依赖调度、并行、重试、超时、取消
  service.py     # FastAPI 接口层
main.py          # 服务入口
tests/           # 全部本地测试
```
