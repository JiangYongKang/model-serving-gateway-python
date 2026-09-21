# 本地模型推理网关（Model Serving Gateway）

一个**仅依赖本地进程**的模型推理网关：用确定性的本地后端模拟多个模型版本，
使**并发合批、版本/别名路由、降级与用量统计**在任意入口下都给出
可预期、可解释的结果。不接入任何外部模型服务、真实凭据或付费资源。

## 运行

```bash
uv sync                       # 安装依赖（fastapi/uvicorn/httpx/pytest）
uv run uvicorn main:app --port 8000
# 或： uv run fastapi run main.py --port 8000
uv run pytest                 # 全部测试（29 项，含并发/重复/超限/切换/降级）
```

## 总体结构

```
app/
  config.py    版本/别名/合批/资源/重试/缓存配置与默认演示配置
  errors.py    可区分原因的错误（reason + HTTP 状态码）
  backend.py   确定性本地后端（故障注入、预热、真实副作用计数）
  registry.py  版本/别名注册表、原子别名切换、下线状态、就绪快照
  batcher.py   动态合批（每 model+version 独立队列/worker，异步派发）
  resources.py 两级（全局/每模型）有界并发与排队、背压
  cache.py     有界 FIFO 结果缓存（键含版本与规范化输入）
  stats.py     守恒的用量统计
  service.py   去重、缓存、重试、降级编排
  app.py       FastAPI：生命周期（预热/优雅停机）、路由、异常映射
```

请求在 `InferenceService` 内的判定顺序：

```
去重(request_id) → 路由(版本/别名/下线) → 缓存(model,version,规范化输入)
   → 合批入队 → 资源槽位（并发/排队背压）→ 本地后端批推理
   → 失败则重试 → 重试耗尽则一次性降级 → 仍不可用则明确失败
```

## 1. 路由与别名规则

- 请求体 `{"model", "version", "input", "request_id"}`：
  - `version` 可填**具体版本号**（如 `v2`）或**别名**（如 `stable`）；
  - 省略时走该模型的 `default` 别名（未配置则指向最低版本）。
- 三类拒绝原因彼此可区分：
  | 情形 | reason | HTTP |
  |---|---|---|
  | 未知模型 | `unknown_model` | 404 |
  | 未知版本/别名 | `unknown_version` | 404 |
  | 版本已下线 | `model_offline` | 410 |
  | 同 request_id 不同负载 | `duplicate_id_conflict` | 409 |
- 别名切换是**一次性快照替换**，`resolve` 在锁内完成：单个请求自始至终
  只使用解析出的版本，**不会跨版本混合结果**；切换到不存在的目标会被拒绝，
  别名保持不变（无半更新）。
- 管理接口：
  - `POST /admin/models/{m}/aliases/{alias}?version=v2`
  - `POST /admin/models/{m}/versions/{v}/offline?offline=true|false`

## 2. 合批与背压配置

`BatchConfig`：
- `window`：合批窗口（秒），窗口内持续收集请求；
- `max_size`：单批最大请求数，达到立即成批（`reason=full`）；
- `max_wait`：单个请求在合批层的等待上限，超时 `wait_timeout`(408)；
- `max_inflight_batches`：每个 (模型,版本) 同时在执行的批数。

隔离与背压：
- **不同模型/版本绝不混入同一批**：每个键一条独立队列和一个常驻收集 worker；
- worker 只“收集 + 派发”，批执行是独立异步任务，真正并发度由
  `ResourcePool` 的两级信号量决定，因此**慢模型不会阻塞其他模型**；
- `ResourceConfig`：`global_concurrency/global_queue`、
  `per_model_concurrency/per_model_queue`、`acquire_timeout`；
- 排队上限按“在批项 + 排队项”整体受限，超限立即 `queue_full`(429)，
  等待槽位超时 `wait_timeout`(408)——**积压有界，内存不无界增长**；
- 批内**逐项结算**：某输入失败只 reject 它自己，同批其他请求正常返回。

## 3. 重试、降级与缓存语义

重试（`RetryConfig.max_retries`）：
- 仅对后端**推理失败**重试；背压/超时/停机类错误不重试；
- 一个请求最终只计一次成功用量；失败尝试仍产生真实后端调用（可在
  `real_inferences` 与后端 `call_count` 中看到）；
- 响应 `attempts` 给出含降级在内的总尝试次数，日志打印每次 retry 判定。

降级（`fallback_chains: {"model:version": "target_version"}`）：
- 主版本重试耗尽后**只跳一次**降级，不再级联；
- 降级目标必须**真实存在、未下线且后端已就绪**，否则返回
  `fallback_unavailable`(503)——**绝不返回过期或伪造结果**；
- 成功时响应带 `"fallback": "<target>"`，输出来自目标版本。

缓存（`CacheConfig.capacity/enabled`）：
- 缓存键 = `(model, version, canonical_input)`，输入经
  **递归排序键的规范化 JSON**，等价输入（如 `{"a":1,"b":2}` 与
  `{"b":2,"a":1}`）共享缓存；
- 键包含版本：**版本或别名变更后解析出的版本号变化，旧缓存不会被命中**；
- 容量有上限，满时按**插入序 FIFO**淘汰最旧项（命中不刷新位置），
  行为确定；失败结果不写缓存。

## 4. 去重与用量统计

- 去重键 = `request_id`：相同标识的**并发**提交共享同一次执行
  （原子“检查并登记”，无 check-then-act 竞态），紧接的**重复提交**
  命中有界的近期完成表（FIFO，默认 4096 条，防内存无界增长）。
- 相同标识只产生**一次真实推理、只计一次用量**；重复响应带
  `"deduplicated": true`，观测计数 `dedup_hits` 增加，但不增加
  received/succeeded。
- 守恒恒等式：`received == succeeded + failed`；
  失败（含降级不可用）计入 `failed`，**不计入成功用量**。
- `GET /v1/stats` 查看总量与逐版本明细：
  `received/succeeded/failed/cache_hits/dedup_hits/retries/
  fallbacks/real_inferences`。

## 5. 预热与优雅停机

- 启动时对所有本地后端执行 `warmup`；`GET /health/ready` 仅在
  **全部后端就绪且未停机**时 `ready=true`，并返回逐后端就绪快照。
- 停机（SIGTERM/SIGINT 或 lifespan 结束）：
  1. 置 `shutting_down`，就绪探针转 false，**新请求立即被拒**
     （`shutting_down`, 503），不挂起；
  2. 等待在途请求在 `shutdown_timeout` 内完成（drain）；
  3. 仍在合批队列中未派发的请求以 `shutting_down` 明确拒绝；
  4. 取消收集 worker、等待在途批结束，无任务/资源泄漏。

## 6. 默认演示配置（可复现）

`app.config.default_config()`：
- `math: v1/v2` 正常；`v3` 对每个唯一输入**首次尝试失败、重试成功**；
  别名 `stable→v1`、`canary→v2`、`edge→v3`；
- `flaky: v1` 恒失败，降级链 `flaky:v1 → v2`（降级成功示例）；
- `doom: v1` 恒失败，降级目标 `v2` **未注册**（降级不可用示例）；
- `slow/fast` 用于验证慢模型不阻塞快模型。

后端输出为
`model/version:<sha256(model|version|规范化输入)[:16]>`，完全确定。

## 7. 验证方法

```bash
uv run pytest                           # 全部用例，-s 打印判定日志
uv run pytest tests/test_routing.py     # 路由/别名/下线原因可区分
uv run pytest tests/test_batching.py    # 合批、跨模型隔离、慢模型隔离
uv run pytest tests/test_backpressure.py# 队列超限/等待超时
uv run pytest tests/test_resilience.py  # 重试、降级成功/不可用
uv run pytest tests/test_cache.py       # 版本隔离、规范化、FIFO、别名切换
uv run pytest tests/test_dedup_stats.py # 并发去重、用量守恒
uv run pytest tests/test_lifecycle.py   # 预热就绪、优雅停机
```

测试用例的标准输出会打印**请求标识、批次号与判定依据**，例如：

```
[合批] request_ids=b-0..b-5 batch_id=1 reason=concurrent_within_window
[背压] 6 请求结果分布={'queue_full': 3, 'wait_timeout': 2, 'ok': 1} ...
[并发去重] request_id=dup-conc x8 -> 真实推理 1 次, 去重命中 7 次 ...
[降级不可用] request_id=fb-2 reason=target_not_registered -> 503 ...
[别名切换] ptr: v1 -> v2 后不命中旧缓存 reason=resolved_version_changed
```

手工验证（实时 HTTP）：

```bash
uv run uvicorn main:app --port 8000 &
curl -s localhost:8000/health/ready
curl -s -X POST localhost:8000/v1/infer -H 'Content-Type: application/json' \
  -d '{"model":"math","version":"stable","input":{"x":1},"request_id":"r1"}'
curl -s localhost:8000/v1/stats
```
