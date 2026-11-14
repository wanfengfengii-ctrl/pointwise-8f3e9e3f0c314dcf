# 职责与依赖边界说明

本次重构只调整职责与依赖边界，不改变任何查询语义、求值顺序、短路规则、错误
优先级或惰性策略。旧的融合解释器被拆成两个可独立驱动的生成器核与一个协调器，
`queryflow.run` 仅做兼容转发。

## 模块与依赖方向

```
queryflow（兼容外壳，仅转发）
   └── coordinator（协调器：路由、句柄、异常、trace 收集）
          ├── exprkernel（表达式核）
          └── relationkernel（关系核）
两个核都只 import 标准库与 protocol；彼此、coordinator、queryflow、samples 均不导入。
protocol 只含 Fault 异常与 SHUTDOWN 哨兵，不含任何执行规则。
```

依赖方向经源码 AST 复核：`exprkernel` 仅依赖 `protocol`；`relationkernel` 仅依赖
`json`、`protocol`；协调器通过模块属性 `exprkernel.evaluate` 与
`relationkernel.execute` 调用，替换任一模块属性后路由保持一致（探针与手工替身
均验证通过）。`queryflow.py` 只剩一个 `run` 函数、无类、无解释器、无备用路径。

## 表达式核 `exprkernel.evaluate(e, env, params, group=None, aggregating=False)`

返回 Python 生成器，`next` 启动、`send` 响应、`throw(protocol.Fault(...))` 注入
业务失败，标量经 `StopIteration.value` 返回。

独占职责：名称查找（限定/非限定、歧义、遮蔽）、标量运算、严格布尔检查、
`and/or/if/coalesce` 短路、聚合参数按成员环境求值及本地聚合上下文、
`scalar/exists/in` 的子查询消费规则（基数、第二行错误优先、IN 提前命中与 NULL
语义）。

它不接收 `tables`、不读表、不创建或调用关系执行器。只发出四类请求：

- `("trace", event)`：含子查询事件，位置与旧代码一致。
- `("open", query, outer, existence)`：以当前环境作为 outer，不携带调用者的
  `group`/`aggregating`；仅创建关系生成器，不推进、不读源。
- `("next", handle)`：消费 `("row", row)` / `("end",)`，失败由协调器 `throw`。
- `("close", handle)`：每次成功 open 恰好 close 一次；open 本身失败时不 close。
  句柄不透明，表达式核只原样携带，不迭代、不解释。

嵌套表达式用标准 `yield from` 委托。业务故障被 `throw` 进活动生成器时，异常沿
委托链直达活跃 yield，各帧 `finally` 中的子查询 `close` 请求再沿同一链条冒泡，
由协调器持续服务直到该帧重新抛出故障——没有生成器被遗弃在 yield 处等待 GC。

## 关系核 `relationkernel.execute(q, outer, tables, existence=False)`

返回 Python 生成器，结束时返回 `None`。

独占职责：表源读取与 `scan` 事件、LATERAL 横向源从左向右枚举、外连接无行补空、
分组（遇键建组、成员保序、空输入 `group=[]` 的空组与合成 null 环境）、
where/having 的严格布尔类型检查与过滤、投影、distinct、稳定排序与 limit（含
`limit=0` 在读源/求值前返回、未排序达到 limit 即停）。`exists` 跳过投影、
distinct、order，但保留 `limit=0` 抑制。

它不求值任何表达式（字面量、列、聚合参数也不求值）。只发出：

- `("eval", e, env, group, aggregating)`：所有值都经此请求取得，回复原样使用；
  新查询从无组上下文开始，where/分组键/普通投影与 having/聚合投影的传参与旧代码
  一致。
- `("trace", event)`：表扫描事件在旧位置发出。
- `("row", row)`：交付最终结果行；协调器再次拉取才继续。

内部横向派生源可递归复用本核，但其行只在核内消费，绝不作为外层 `row` 交付。
核内用 `_Env/_Candidate/_Row` 标记在阶段间传递，与对外协议元组区分。

## 协调器 `coordinator.run(query, tables, params=None)`

独占：活生成器、不透明关系句柄、请求路由、异常注入、trace 收集与最终结果/错误
拼装。它只读取顶层 select 的输出名，不解释任何查询键、表达式标签、运算符或行值。

- 响应 `eval`：调用 `exprkernel.evaluate`；响应 `open/next/close`：管理
  `relationkernel.execute` 的句柄（含父子后代关系）。
- 故障纪律：关系返回的业务故障被 `throw` 回活动的表达式生成器，并持续驱动它，
  服务其 `finally` 发出的 `close`/`trace`，直到生成器重新抛出故障并终止；标准
  `yield from` 让故障穿过每一层嵌套表达式帧有序展开。关系自身在请求间抛出的
  故障同样上抛给请求方表达式。
- 关闭纪律：仍存活的关系句柄在收尾时用 `protocol.SHUTDOWN` 哨兵做业务级显式
  关闭，深度优先、先后代后自身。关闭不拉行、不求值、不新增 trace。正常结束、
  提前命中（IN、短路、limit）与异常路径都保证每个成功 open 恰好一次 close。
- 成功返回 `{"columns", "rows", "trace"}`；业务异常返回
  `{"error", "trace"}`，不返回部分 rows。

### 为什么需要 SHUTDOWN 哨兵

关系核内部存在多层嵌套生成器阶段（source/candidates/projected/unique/排序）。
若依赖 GC 的 `GeneratorExit` 关闭一个停在 yield 的生成器，而其下游帧的 `finally`
仍需 yield，Python 会抛出 "generator ignored GeneratorExit"。因此关闭被建模为
显式数据驱动：协调器发送共享哨兵，每个驱动循环识别后立即停止，并在自己的清理
中把哨兵转发给所拥有的下级阶段。整个过程不经过 `GeneratorExit`，也不产生额外
读取或 trace。

## 上下文、拉取与关闭归属

| 主题 | 表达式核 | 关系核 | 协调器 |
| --- | --- | --- | --- |
| 环境帧/成员 group | 只读使用，按成员聚合 | 构建 env、group 与空组合成帧 | 不解释内容，仅随 eval/open 透传 |
| 表 | 不可见 | 独占读取并记 scan | 仅保存 tables 映射传给关系核 |
| 拉取 | 经 next 消费关系，IN/scalar 按规则停 | 惰性产出 row，limit 提前停止 | 驱动 next，行只在再次拉取后继续 |
| 子查询生命周期 | 决定 open/next/close 时机与消费规则 | 被 open 时只创建、不推进 | 持有句柄，故障时收尾、SHUTDOWN 关闭 |
| 异常 | 抛出/经 throw 传播并在 finally 关闭子查询 | 表/布尔等业务 Fault 上抛 | 注入故障、服务清理、收集 error+trace |
| trace | 发 subquery 等表达式侧事件 | 发 scan 事件 | 唯一收集者，保证顺序与重复次数 |

关联子查询的每个成员环境与本地聚合上下文由关系核构建、表达式核只读使用；进入
子查询即重建聚合上下文，不捕获外层 group。共享查询 dict、表达式 list 与递归引用
环原样复用，不预展开、不改写；实测 shared(8) 一次执行只打开 17 个互异查询对象，
递归用例正常终止，输入对象图执行前后不变。

## 验证与回归结果

实际执行（`python3 checks.py` 为 verify 清单，默认入口先跑原业务回归再跑
`boundary_checks.regression`）：

- `python3 checks.py` →
  `BUSINESS_REGRESSION_OK`、`ADDITIONAL_BUSINESS_OK`、`BOUNDARY_REGRESSION_OK`，退出 0。
- `python3 boundary_checks.py business` → `ADDITIONAL_BUSINESS_OK`，退出 0。
- `python3 boundary_checks.py` → `BOUNDARY_REGRESSION_OK`，退出 0
  （含禁止相互导入的隔离导入、表达式脚本化关系响应、关系非解释型替身响应、
  协调器替换任一核的路由探针）。
- `python3 run.py` 输出 TOTALS；`python3 checks.py demo` 输出 ENRICH/TOTALS、
  shared(8) 与 recursive 的真实结果。
- 针对 git 中旧解释器的差分模糊：两组随机种子共 10,000 个随机查询（含嵌套子查询、
  聚合、排序、limit、缺失表/列/参数、各类运行错误），结果与 trace 全部一致，
  且查询图、表、参数均未被修改。
- 重复执行成功与失败用例后，进程内无残留存活生成器；深 LATERAL 故障与聚合关联
  子查询故障均无生成器泄漏，关闭不新增 trace，错误响应不含部分 rows。
- 保留了 `checks.py` 的全部既有断言，仅在默认入口尾部追加边界回归调用。
