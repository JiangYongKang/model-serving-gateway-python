# 本地模型推理网关

一个仅依赖本地进程的模型推理网关：用**确定性本地后端**模拟多个模型版本，
在并发合批、版本路由、降级与用量统计等能力上给出可预期、可解释的结果。
不接入任何外部模型服务、真实凭据或付费资源。

## 运行

```bash
uv sync
uv run uvicorn main:app          # 启动 HTTP 服务（演示配置见 gateway/demo.py）
uv run pytest tests/ -q          # 运行测试
uv run pytest tests/ -q -s       # 查看测试日志（请求标识、批次、判定依据）
```

HTTP 接口：

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/infer` | 推理：`{"model", "prompt", "version"?, "request_id"?}` |
| GET | `/ready` | 就绪状态（反映后端真实可用性） |
| GET | `/stats` | 用量统计 |
| POST | `/admin/alias` | 切换别名：`{"model", "alias", "version"}` |
| POST | `/admin/weights` | 灰度放量：`{"model", "alias", "weights": {"v1": 1, "v2": 3}}` |
| POST | `/admin/rollback` | 一键回退：`{"model", "alias", "version"}`，返回被撤销版本列表 |
| GET | `/stats/versions` | 分版本统计（承接/成功/失败/降级/真实推理） |
| POST | `/admin/retire` | 下线版本：`{"model", "version"}` |

错误响应统一为 `{"detail": {"reason": <原因码>, "message": <说明>}}`。

## 路由与别名规则

- 请求可指定**精确版本**或**别名**；精确版本优先于同名别名；未指定时使用别名 `latest`。
- 单个请求只在入口解析一次别名，之后固定使用该版本——**别名切换过程中单请求绝不跨版本混合**。
- 拒绝原因可区分：
  - `unknown_model`（404）：模型不存在；
  - `unknown_version`（404）：版本/别名不存在，或别名指向缺失版本；
  - `version_retired`（410）：版本已下线（含别名指向已下线版本）；
  - `no_active_version`（503）：别名下无可用版本（全部权重为 0 / 已止损 / 已下线）。

## 灰度放量、自动止损与一键回退

- **权重分流**：`/admin/weights`（或 `Registry.set_alias_weights`）让别名同时指向多个版本，
  新到达的请求按最新权重随机分流；权重为 0、已下线或已止损的版本不再接到新请求。
  单请求从进入到返回只归属一个版本，绝不跨版本拼接或中途换版本。
  旧配置兼容：静态 `aliases` 与 `/admin/alias` 的单版本写法等价于权重 `{version: 1}`，行为不变。
- **分版本观测**：`GET /stats/versions` 给出每版本的承接数、成功、失败、拒绝、
  降级次数与真实推理次数；各版本合计与 `/stats` 整体口径一致
  （降级产生的推理归属实际产出结果的版本，守恒在并发、重复提交与降级场景下都成立）。
- **自动止损**：给版本配置 `BreakerSpec(failure_ratio, window, min_requests)`，
  最近 `window` 个已完成请求中失败比例达到 `failure_ratio`（且样本数 ≥ `min_requests`）时，
  自动停止该版本的放量，其余版本继续正常服务。止损只影响新请求的分流，
  在途请求正常收尾并继续计数。调整权重**不会**解除止损（防止无意恢复），
  明确恢复用 `Registry.resume_version`。
- **一键回退**：`/admin/rollback`（或 `Gateway.rollback`）把别名收敛回只由一个确定版本承接。
  回退后新请求不再进入被撤销版本，其此前产生的缓存结果与重复提交（去重）结果立即失效、
  不再被返回；回退不影响在途请求，也不改变重试、降级、缓存容量上限与排队行为。

## 合批与背压配置

`BatchConfig`：
- `window_ms`：合批窗口，窗口内的同 `(model, version)` 请求合并成批；
- `max_batch_size`：批大小上限，达到立即冲刷；
- `max_wait_ms`：单请求等待上限，有效窗口 = `min(window_ms, max_wait_ms)`。

保证：不同模型或版本**绝不混入同一批**；批内单项失败（`ItemFailure`）只落在该项上，
不污染同批其他请求。

`AdmissionConfig`：
- `global_concurrency` / `global_queue`：全局并发与排队上限；
- `per_model_concurrency` / `per_model_queue`：每模型并发与排队上限（慢模型不阻塞其他模型）；
- `wait_timeout_ms`：排队等待超时。

并发槽满则排队，队列满立即以 `queue_full`（429）拒绝；排队超时以 `wait_timeout`（429）拒绝。
计数全部有界，积压不会无界增长内存。

## 重试、降级与缓存语义

- **重试**（`RetryConfig.max_attempts`）：仅针对后端整体故障（`BackendError`）；
  单项确定性失败（`ItemFailure`）不重试不降级。重试**不重复计入用量**（`real_inferences` 只计一次）。
- **降级**：重试耗尽后按 `ModelSpec.fallback` 顺序尝试降级版本；已下线目标真实跳过；
  降级链全部不可用时以 `fallback_unavailable` 明确失败，**绝不返回过期或伪造结果**；
  无降级链时以 `backend_error` 失败。
- **缓存**：键 = `(model, version, normalize(prompt))`（规范化 = 去首尾空白、折叠内部空白、统一小写）。
  版本或别名变更后键自然变化，**旧缓存不会被命中**。容量有上限（`CacheConfig.capacity`），
  LRU 确定性淘汰最久未用项。降级产生的结果按**实际产生结果的版本**写入缓存。

## 去重与用量统计

- 相同 `request_id` 的提交（并发在途或已完成）只产生**一次真实推理**：
  在途共享同一 future，已完成直接返回记录的结果（有界 LRU，容量 1024）。
- 统计守恒：`accepted = succeeded + failed + rejected`。
  - `failed` 仅计后端/降级失败；路由、背压、生命周期拒绝计 `rejected`；
  - 失败请求**不计入成功用量**；重试/降级/去重不重复计 `real_inferences`。

## 生命周期

- `start()` 预热：对每个未下线版本后端做一次真实探测，全部可用才就绪；
  未就绪时请求以 `not_ready`（503）拒绝。
- `shutdown()` 优雅停机：立即停止接收新请求（`shutting_down`，503），
  等待在途请求完成后冲刷合批器，无半更新状态与资源泄漏。

## 验证方法

```bash
uv run pytest tests/ -q -s
```

测试以并发、重复提交、队列超限、版本切换与降级为主，日志中打印
请求标识、批次与判定依据：

- `test_routing.py`：未知模型/版本/已下线原因可区分；别名切换不跨版本混合；
- `test_batching.py`：并发合批（后端调用次数 < 请求数）；不同版本不混批；单项失败不污染批；
- `test_admission.py`：排队超限明确拒绝；慢模型不阻塞快模型；
- `test_fallback.py`：重试计一次用量；降级到可用版本；降级不可用明确失败；
- `test_cache.py`：规范化命中；版本变更使旧缓存失效；容量上限与确定性淘汰；
- `test_dedup.py`：并发/顺序重复提交只真实推理一次；混合场景统计守恒；
- `test_lifecycle.py`：未就绪拒绝；在途请求完成后停机；停机期间新请求拒绝；
- `test_rollout.py`：权重分布接近配置；并发下权重热更新分流一致；权重 0 版本不接新请求；
  自动止损触发时在途请求正常收尾；回退后旧版本缓存/去重结果不再命中；
  并发+去重+降级混合场景下分版本统计与整体守恒；
- `test_api.py`：HTTP 各入口与错误原因码；灰度放量/回退/分版本统计端点。

本地复现灰度与回退（无需启动服务）：

```bash
uv run pytest tests/test_rollout.py -q -s   # 日志含权重分布、止损与回退的判定依据
```

## 代码结构

```
gateway/
  config.py     配置（模型/版本/别名/降级链/合批/背压/缓存/重试/止损）
  errors.py     原因码与 GatewayError
  registry.py   路由与别名（多版本权重分流、止损、回退，运行期可热更新）
  rollout.py    分版本统计与滑动窗口自动止损
  backends.py   确定性本地后端（故障可注入）
  batching.py   动态合批器
  admission.py  资源隔离与背压
  cache.py      有界 LRU 结果缓存（可按版本失效）
  dedup.py      请求去重与用量统计（结果记录来源版本，可按版本失效）
  gateway.py    编排：路由→缓存→去重→背压→合批→重试/降级→统计
  app.py        FastAPI 入口
  demo.py       演示配置
```
