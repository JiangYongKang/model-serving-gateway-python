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
| GET | `/stats` | 整体用量统计 |
| GET | `/stats/versions` | 分版本观测统计（承接/成功/失败/拒绝/降级/实际推理） |
| POST | `/admin/alias` | 切换别名（单版本，等价权重 1）：`{"model", "alias", "version"}` |
| POST | `/admin/weights` | 灰度放量：`{"model", "alias", "weights": {"v1": 1, "v2": 3}}` |
| POST | `/admin/rollback` | 一键回退：`{"model", "alias", "version"}`，返回被撤销版本列表 |
| POST | `/admin/breaker` | 配置自动止损：`{"model", "version", "failure_ratio", "window"}` |
| POST | `/admin/retire` | 下线版本：`{"model", "version"}` |

错误响应统一为 `{"detail": {"reason": <原因码>, "message": <说明>}}`。

## 路由与别名规则

- 请求可指定**精确版本**或**别名**；精确版本优先于同名别名；未指定时使用别名 `latest`。
- 单个请求只在入口解析一次别名，之后固定使用该版本——**别名切换/权重调整过程中单请求绝不跨版本混合**。
- 拒绝原因可区分：
  - `unknown_model`（404）：模型不存在；
  - `unknown_version`（404）：版本/别名不存在，或别名指向缺失版本；
  - `version_retired`（410）：版本已下线（含别名指向已下线版本）；
  - `version_tripped`（503）：别名下所有版本均已触发自动止损。

## 灰度放量（加权多版本分流）

- 别名可同时指向多个版本并按权重分流：`ModelSpec.alias_weights = {"canary": {"v2": 9, "v1": 1}}`，
  或运行期 `POST /admin/weights` 热更新；新到达的请求立即按最新配置分流。
- 权重为 0、已下线、已触发止损的版本不参与分流；其余版本按剩余权重比例承接。
- 旧配置完全兼容：`aliases: {"latest": "v2"}` 与 `POST /admin/alias` 等价于单版本权重 1。
- 足够多的独立请求的分流分布接近配置权重（见 `test_rollout.py` 分布断言）。

## 自动止损（分版本观测 + 熔断）

- 分版本观测（`GET /stats/versions`）：每个版本承接请求数、成功、失败、拒绝、
  降级次数与实际执行的推理次数；各版本合计与整体口径一致（守恒），
  在并发、重复提交与降级场景下同样成立。
- 止损配置（`ModelSpec.breakers` 或 `POST /admin/breaker`）：`failure_ratio` +
  `window`（最近 N 个终态请求，排队拒绝不计入窗口）。窗口内失败比例达到阈值即
  自动把该版本从所有别名分流中摘除，其余版本继续正常服务。
- 止损只影响新请求的分流选择：**在途请求正常结束**，不会出现半更新或计数错乱。
- `Gateway.reset_breaker(model, version)` 可解除止损状态（需另行恢复权重才重新放量）。

## 一键回退

- `POST /admin/rollback {"model", "alias", "version"}`：立即把**目标别名**收敛为只由指定版本承接，
  返回该别名上被撤销的版本列表。
- **作用范围只限目标别名**，不做任何全局状态变更：
  - 其它别名（包括另一个仍指向被撤销版本的别名）与回退前完全一致，继续承接同一版本，正常返回；
  - 被撤销版本**不会被下线**：显式指定该版本、或经由其它别名访问照常服务；
  - **降级链不受影响**：保留版本后端故障时仍按配置降级到被撤销但仍可用的版本并成功返回，
    只有降级目标确实都不可用时才以 `fallback_unavailable` 明确失败；
  - 重试、缓存容量上限与排队行为保持不变。
- 被撤销版本此前产生的**缓存结果与重复提交（request_id）记录被清除**，不会再被返回；
  以“版本代际”实现：回退使被撤销版本代际 +1，旧代际下尚未结束的在途去重记录会被**封闭**——
  已在等待的提交照常拿到在途结果收尾，回退之后才到达的同一 request_id 提交在保留版本上重新执行。
- 回退不影响正在处理中的请求（它们已完成路由解析，正常结束），分版本与整体计数保持守恒。

## 灰度下的重复提交幂等语义

- 相同 `request_id` 的去重作用域是 **(model, request_id)，与落到哪个版本无关**：
  灰度加权分流、权重热更新期间，同一 `request_id` 的多次提交（含并发落到不同版本）
  **只真实推理一次**，所有提交得到**同一个结果**（跟随领导者的同一次执行）。
- 权重热更新不改变这一语义；只有回退（语义边界变更）才会使被撤销版本的旧结果失效
  并允许同一 `request_id` 在保留版本上重新执行（见“一键回退”）。
- 不同 `request_id` 但规范化后相同的输入，仍按 `(model, version, prompt)` 共享版本缓存。

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

- 相同 `request_id` 的提交（并发在途或已完成）只产生**一次真实推理**，
  作用域为 `(model, request_id)`，**跨灰度版本同样成立**：
  在途共享同一 future，已完成直接返回记录的结果（有界 LRU，容量 1024）；
  回退会按版本代际清除/封闭旧结果（见“一键回退”）。
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

针对本次修复场景的本地手工复现（先 `uv run uvicorn main:app` 启动演示服务）：

```bash
# 1) 灰度别名两版本加权；另一个别名 stable 单独指向旧版本
curl -s localhost:8000/admin/weights -H content-type:application/json \
  -d '{"model":"echo","alias":"canary","weights":{"v1":1,"v2":1}}'
curl -s localhost:8000/infer -H content-type:application/json \
  -d '{"model":"echo","prompt":"p","version":"stable","request_id":"a"}'   # 仍走 v1

# 2) 同一 request_id 并发重复提交（可在两个终端同时执行），结果一致、只真实推理一次
curl -s localhost:8000/infer -H content-type:application/json \
  -d '{"model":"echo","prompt":"p","version":"canary","request_id":"dup"}'
# 观察 GET /stats/versions：real_inferences 只增加一次；GET /stats 中 dedup_hits 增加

# 3) 一键回退 canary 到 v1
curl -s localhost:8000/admin/rollback -H content-type:application/json \
  -d '{"model":"echo","alias":"canary","version":"v1"}'                   # {"revoked":["v2"]}

# 4) 未参与回退的别名与显式访问照常服务
curl -s localhost:8000/infer -H content-type:application/json \
  -d '{"model":"echo","prompt":"p","version":"stable","request_id":"b"}'  # 仍正常返回 v1 结果
curl -s localhost:8000/infer -H content-type:application/json \
  -d '{"model":"echo","prompt":"p","version":"v2","request_id":"c"}'      # 显式 v2 仍可用

# 5) 旧 request_id 经回退后别名重新提交：不再返回旧 v2 结果，而在 v1 重新执行
curl -s localhost:8000/infer -H content-type:application/json \
  -d '{"model":"echo","prompt":"p","version":"canary","request_id":"dup"}' # 结果前缀变为 echo@v1#

# 6) 自动测试稳定复现并断言以上行为（含回退后降级链与在途收尾/统计守恒）
uv run pytest tests/test_rollout.py -q -s
```

测试以并发、重复提交、队列超限、版本切换与降级为主，日志中打印
请求标识、批次与判定依据：

- `test_routing.py`：未知模型/版本/已下线原因可区分；别名切换不跨版本混合；
- `test_rollout.py`：权重分流分布接近配置；并发下权重热更新单请求不跨版本、权重 0 停止接流；
  自动止损触发后在途请求正常收尾、其余版本继续服务；
  **回退作用域只限目标别名（其它别名继续承接同一版本、显式访问仍可用）；
  回退后保留版本故障仍能降级到被撤销版本、目标全不可用才明确失败；
  灰度（含随机加权并发）下同一 request_id 跨版本只真实推理一次且结果一致；
  权重热更新后重复提交仍只推理一次；回退后旧版本缓存/去重结果不再命中；
  回退瞬间在途去重被封闭（已等待者收尾、回退后的重复提交重新执行）；
  在途请求收尾后分版本统计与整体统计在并发/重复提交/降级/背压下守恒**；
- `test_batching.py`：并发合批（后端调用次数 < 请求数）；不同版本不混批；单项失败不污染批；
- `test_admission.py`：排队超限明确拒绝；慢模型不阻塞快模型；
- `test_fallback.py`：重试计一次用量；降级到可用版本；降级不可用明确失败；
- `test_cache.py`：规范化命中；版本变更使旧缓存失效；容量上限与确定性淘汰；
- `test_dedup.py`：并发/顺序重复提交只真实推理一次；混合场景统计守恒；
- `test_lifecycle.py`：未就绪拒绝；在途请求完成后停机；停机期间新请求拒绝；
- `test_api.py`：HTTP 各入口与错误原因码。

## 代码结构

```
gateway/
  config.py     配置（模型/版本/别名/权重/止损/降级链/合批/背压/缓存/重试）
  errors.py     原因码与 GatewayError
  registry.py   路由与别名（加权分流、运行期可切换、可下线、止损摘除、按别名回退）
  rollout.py    分版本观测统计与自动止损（熔断）
  backends.py   确定性本地后端（故障可注入）
  batching.py   动态合批器
  admission.py  资源隔离与背压
  cache.py      有界 LRU 结果缓存（可按版本清除）
  dedup.py      请求去重（按 model+request_id，跨灰度版本；版本代际支持回退封闭）与用量统计
  gateway.py    编排：路由→缓存→去重→背压→合批→重试/降级→统计
  app.py        FastAPI 入口
  demo.py       演示配置
```
