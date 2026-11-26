# data-qd-240 · IPv4 目的路由、UDP 出口与批次承诺

用户态测试转发器接收 IPv4 目的地址和字节载荷；按最长匹配前缀选择命名出口，
或在该前缀配置丢弃时不发送。不匹配返回 `unrouted`。命中命名出口时通过真实
UDP socket 发送；发送成功只表示本机 `sendto` 调用成功，不是接收确认。

所有出口必须是 IPv4 回环地址。项目仅用于本机回归：使用合成字节和临时回环
UDP 接收器，不接入真实流量，不做探测、扫描、重发、广播或对外转发。保持
Python 3.9+ 标准库。

## 原有直接发送接口

```python
with Forwarder({"east": ("127.0.0.1", east_port)}) as fw:
    fw.replace([("0.0.0.0/0", "east")])
    outcome = fw.send("203.0.113.10", b"sample")
    # Outcome(action="sent", prefix="0.0.0.0/0", egress="east")
```

`send(destination, payload)` 的行为保持不变：

- `destination` 必须是 IPv4 文本；
- `payload` 必须是 `bytes` 且长度不超过 4096；
- 每次调用按当时活动表做最长前缀决策；
- `drop` 和 `unrouted` 不发送；
- `sendto` 抛出的 `OSError` 原样传播，不尝试其他出口；
- 直接发送不占批次容量，也不受批次承诺保护。

## 离线定容查表镜像

`Forwarder.compile_offload(offload_ranges, max_rows)` 从调用时的一致活动表
快照编译一个可独立使用的 `OffloadPlan`。新接口只做查表和判定，不调用
`send`/`dispatch_next`，不发送数据报。

```python
ranges = [
    ("192.0.2.0/24", True),   # 允许快路径回答整个 /24
    ("192.0.2.16/28", False), # 但这个例外洞必须交给软件
]
plan = fw.compile_offload(ranges, 3)

plan.rows
# 例如：
# (("192.0.2.0/24", "drop", None),
#  ("192.0.2.0/25", "sent", "east"),
#  ("192.0.2.16/28", "punt", None))

from forwarding import lookup_image
lookup_image(plan.rows, "192.0.2.17")
# ("punt", None)
```

### 行格式

`plan.rows` 是不可变的 `tuple`，其中每行是三元组：

```text
(CIDR 文本, action, egress)
```

- CIDR 是严格、规范的 IPv4 前缀文本（主机位会在输入校验阶段被拒绝）；
- 镜像内前缀唯一，允许 `0.0.0.0/0`，也允许显式更具体的 `punt` 行；
- `action` 与 `egress` 取值如下：

| `action` | `egress` | 含义 |
| --- | --- | --- |
| `"sent"` | 命名出口字符串 | 最长前缀选中了该出口；仅表示策略判定，不表示发了包 |
| `"drop"` | `None` | 原表显式丢弃 |
| `"unrouted"` | `None` | 允许区内但原表无路由 |
| `"punt"` | `None` | 不允许快路径给出确定结论，必须交给软件 |

独立函数 `lookup_image(rows, destination) -> (action, egress)` 只依赖传入的
导出行和 IPv4 文本，不依赖原表、范围定义或存活的 `Forwarder`。它用镜像自身
做最长前缀匹配；没有任何行命中时返回 `("punt", None)`。镜像覆盖完整
`0.0.0.0/0`–`255.255.255.255` 地址空间，而不是只覆盖示例网段。

### 范围选择与校验

`offload_ranges` 是 `(严格 IPv4 CIDR 文本, bool)` 的可迭代对象：

- 布尔授权也按最长前缀匹配，未命中的布尔默认是 `False`；
- 前缀顺序不影响语义，可嵌套授权与排除；
- 拒绝重复前缀、带主机位的 CIDR、非 IPv4 前缀和非 `bool` 值；
- 是否允许快路径只由这些范围决定，与路由动作、出口名或票据无关。

编译规模上限为：

- 至多 4096 条活动路由；
- 至多 4096 条范围规则；
- 至多 32 个已配置出口；
- `max_rows` 必须是 `0..8192` 的整数，`bool` 不被当作整数。

非法输入或超过上述编译规模时抛出 `ValueError`；这些上限不改变原有
`replace`、`send` 或批次入口的行为。

### 预算与失败行为

编译器在压缩二进制前缀 Trie 上做精确动态规划，因此会得到全域正确镜像所需的
最少行数（实现直接输出该最优行数）：

- 最少行数不超过 `max_rows` 时成功，`len(plan.rows) <= max_rows`；
- 不存在满足预算的正确镜像时，抛出可从 `forwarding` 导入的
  `OffloadBudgetExceeded`，不返回截断镜像，也不会偷偷把允许区改成 `punt`；
- 异常提供 `budget`/`max_rows` 和 `minimum_rows` 诊断属性；
- 空范围集的结果是空行元组，因此预算 `0` 也能成功；它表示所有地址都 `punt`。

`OffloadBudgetExceeded` 同时继承 `ForwarderError` 和 `ValueError`，但调用方
通常应显式捕获这个专用类型。

### `plan.resolve(destination, *, software_available=True)`

返回 `(source, action, egress)`，其中 `source` 是 `"fast"` 或
`"software"`：

1. 始终先查离线镜像；
2. 镜像结果不是 `"punt"` 时，直接返回 `source="fast"`，不查询软件；
3. 镜像结果是 `"punt"` 且 `software_available=True` 时，调用编译时捕获的
   同一份活动表上的原最长前缀选路，返回 `source="software"`；
4. 镜像结果是 `"punt"` 且 `software_available=False` 时，抛出可从
   `forwarding` 导入的 `SoftwareUnavailable`，不会改报 `unrouted`，也不使用
   缓存出口。

`lookup_image` 和 `plan.resolve` 都要求 IPv4 文本；非法文本（包括非字符串、
IPv6、带 `/mask` 的地址等）抛出 `ValueError`。两条路径都不触碰 UDP 发送。

### 快照、锁与 `replace`

`compile_offload` 只在短暂进入转发器状态锁时取得不可变的 `_routes` 元组引用；
范围校验和 Trie 编译均在锁外执行，不会在整个编译期间阻塞 `replace` 或票据
状态转换。

`OffloadPlan` 的导出行和软件兜底都使用这同一份表快照。之后调用
`replace` 会增加修订号并更新活动表，但已经发出的旧计划仍按旧表做软件选路；
只有新调用的 `compile_offload` 才使用新表。编译成功或预算/输入失败都不会修改
当前活动表、修订号或票据。

## 批次托管接口

### `ticket = reserve(samples)`

`samples` 是按批内顺序给出的 `(destination, payload)` 可迭代对象。受理过程：

1. 物化并检查批次大小；
2. 复用 `send` 的 IPv4 文本和载荷限制；
3. 在与 `replace` 相同的状态锁内，用同一份活动表重新做完整最长前缀判断；
4. 全部条目都命中命名出口且容量足够时，才登记票据和容量。

限制：

- 每批最多 16 条；
- 当前实例已受理但未结束的样本（包括已领取、正在发送的样本）合计最多 64 条；
- 容量不足时整批拒绝，不会只受理一部分；
- `reserve` 成功不会发送任何数据报。

返回的 `BatchTicket` 是不透明、实例独有且进程内唯一的票据；调用方不应依赖
其字段。只读诊断属性 `active_sample_count` 返回当前已受理但未结束的样本数
（包含正在发送的样本）。

### `dispatch_next(ticket)`

每次显式推进批内下一条，按原始顺序调用真实 UDP 发送。成功时返回受理时固化
的结果：

```python
Outcome("sent", accepted_prefix, promised_egress_name)
```

注意：返回的是**受理时命中的前缀和承诺的命名出口**，不是换表后的新前缀。
每条最多实际尝试一次。

- 发送成功后立即释放该条容量和路由保护；
- 最后一条成功后票据立即失效；
- `OSError` 原样抛给调用方；该票据终止，已领取的失败条和所有余项立即释放，
  余项不会再发送，也不能重试该票据；
- 之前已经成功的条目不受影响、不撤销。

### `cancel(ticket)`

取消票据中尚未发出的余项，返回取消条数。已成功发送的条目不会补发或撤销；
调用成功后票据失效。

如果同一票据已有发送被领取但尚未从 `dispatch_next` 返回，再次
`dispatch_next(ticket)` 或调用 `cancel(ticket)` 都会立即抛出 `BatchBusy`。
不同票据之间不互斥；没有后台发送线程。

未知、已失效、已结束或属于另一个 `Forwarder` 实例的票据，`dispatch_next` 和
`cancel` 都抛出 `UnknownTicket`（它是 `LookupError` 的子类），并且不会调用
发送 seam 或 socket。

## 如何区分受理拒绝

所有批次受理拒绝都是 `BatchRejected`（同时是 `ValueError` 和
`BatchError`），可用异常类型、`reason` 和 `index` 区分。`index` 是从零开始的
被拒条目位置；批次级问题的 `index` 为 `None`。别名 `position`、
`entry_index`、`sample_index`、`rejected_index` 指向同一值。

| 情况 | 主要类型 | `reason` | 位置/附加信息 |
| --- | --- | --- | --- |
| 空批 | `EmptyBatch` | `"empty"` | `index is None` |
| 非 IPv4 文本、载荷类型/长度错误、条目不是二元组 | `InvalidBatchEntry` | `"invalid"` | `index`；原始异常为 `__cause__`，多个输入错误在 `failures` |
| 命中丢弃规则 | `BatchRouteRejected` | `"drop"` | `index`、`prefix`、`outcome` |
| 无路由 | `BatchRouteRejected` | `"unrouted"` | `index`、`outcome` |
| 单批超过 16 条 | `BatchTooLarge` | `"too_large"` | `CapacityError` 子类，`limit=16` |
| 实例活动样本超过 64 条容量 | `CapacityError` / `BatchCapacityError` | `"capacity"` | `requested`、`available`、`active`、`limit=64` |

输入无效、容量不足和路由承诺冲突是不同异常族：

- 输入/路由受理拒绝：`BatchRejected` / `ValueError`；
- 容量不足：`CapacityError`（仍是 `BatchRejected`，但可单独捕获）；
- 承诺冲突：`RouteProtected`，不是 `ValueError`；
- 同票并发：`BatchBusy`；
- 票据未知：`LookupError` / `UnknownTicket`。

## `replace(routes)` 与只读 `revision`

`replace(routes)` 保持原有行为和成功返回（`None`）：完整校验严格 CIDR、重复
前缀和出口名称；任何输入无效都不会半表替换。

新增只读属性：

```python
fw.revision  # 初始为 0
```

每次成功的 `replace` 都令 `revision` 加一，即使新旧表内容完全相同。校验失败
或因批次承诺冲突而拒绝时，活动表和 `revision` 均不变。

### 承诺保护

发布候选表前，转发器会用候选表对所有尚未结束的托管样本重新做最长前缀判断：

- 新前缀可以变化；
- 命中的命名出口必须与受理时承诺的名字**完全相同**；
- 出口名字不同即冲突，即使两个名字配置成完全相同的回环地址和端口；
- 改成 `drop` 或变成无路由也是冲突。

冲突时抛出 `RouteProtected`，其属性包括：

- `revision`：拒绝时仍保持不变的当前修订号；
- `conflicts`：一个列表，每项含 `ticket`、`index`、`destination`、
  `promised_prefix`、`promised_egress`、`in_flight`、
  `candidate_action`、`candidate_prefix`、`candidate_egress`。

发生 `RouteProtected` 时不会取消票据或余项；冲突消除（例如相应样本发送结束、
被取消或票据失败终止）后，调用方可以用同一张候选表重试。

### 选择的在途运维行为：立即拒绝

本实现选择题目允许的**立即行为**：已经领取但实际发送尚未返回的样本仍然是有效
承诺。如果候选表只与这种在途样本冲突，`replace` 也会立即抛出
`RouteProtected`，不会等待两秒，也没有等待超时或修订冲突流程。调用方观察到
`dispatch_next` 返回/抛错并完成状态释放后，再重新发起同一次换表即可。

因此：

- 不存在“等待期间被兼容发布抢先”的内置等待状态；
- `RevisionConflict` 仅作为保留异常类型提供，当前立即策略不会抛出它；
- 不兼容表不可能在原发送调用返回前发布，因为本实现根本不会在该发送未结束时
  发布它。

## 并发模型

一个 `threading.RLock` 线性化以下状态转换：

- `reserve` 的路由判断、容量检查和票据登记；
- `replace` 的承诺冲突检查与整表发布/修订号递增；
- 票据条目的领取、忙状态、成功完成和失败终止；
- `cancel` 与票据查询。

真实 `sendto`（以及测试 seam `_send_datagram`）在锁外执行，因此某个发送停住
时：

- 兼容的 `replace` 仍可完成并递增修订号；
- 其他票据仍可独立推进；
- 同一票据的第二次推进/取消能立即得到 `BatchBusy`；
- `reserve` 不会被网络/夹具发送阻塞。

`reserve` 与 `replace` 的线性化点都在同一状态锁内，所以不会出现“新表已经
发布，却又按旧表登记承诺”的混合结果。

退出 `with Forwarder(...)` 上下文时会放弃所有未发余项、清空票据并关闭 socket。
约定调用方先收束自己的并发调用；若仍有发送，关闭 socket 可能使该发送收到
`OSError`。

## 测试与复现

```sh
python3 -m unittest -v
python3 -m unittest -v test_batch_forwarding.py
python3 -m unittest -v test_offload.py
python3 demo.py

# 规模用例可在 192 MiB 虚拟地址空间限制下复现：
(ulimit -v 196608; python3 -m unittest -v test_offload.py)
```

`test_forwarding.py` 保留原有回归；`test_batch_forwarding.py` 覆盖：

- 场景 1：同一候选表在有票时被 `RouteProtected` 拒绝、票据结束后发布成功；
  直接发送与托管发送分别返回新前缀和受理前缀；同地址别名不算同一出口；
- 场景 2：用可控发送夹具停在 `_send_datagram` 边界，验证同票忙、不重复发送、
  其他票据可推进、兼容换表可发布、不兼容换表立即拒绝，以及释放后重试成功；
- `reserve` 与不兼容 `replace` 的确定性先后次序和 100 轮并发不变量；
- 拒绝位置/原因、空批、16 条和 64 条容量、成功/取消释放；
- 第二条 `OSError` 终止票据、第三条不发送、原异常对象可见、保护释放；
- 非法 `replace` 保留活动表、修订号和票据；
- 未知/跨实例票据抛出 `LookupError` 且不触发发送；
- 上下文退出放弃余项并关闭 socket。

`test_offload.py` 覆盖离线演练验收：

- `192.0.2.0/24` 的 east/drop 策略、`192.0.2.16/28` 排除洞、预算 2 失败/
  预算 3 成功；联合固定区间边界与导出行边界逐段调用独立的
  `lookup_image` 核对；
- 同一 `192.0.2.17` 在有洞且软件在线时走软件 east、软件停用时报
  `SoftwareUnavailable`，移除排除后软件停用也能走快路径 east；
- 授权无路由网段在软件停用时由快路径给出 `unrouted`，相邻未授权地址仍
  `punt`/报 `SoftwareUnavailable`；
- 旧 `OffloadPlan` 在 `replace` 后仍使用旧表做软件兜底，新编译才使用新表；
- 空范围 + 预算 0、单个无路由 `/32` 在预算 0 下报
  `OffloadBudgetExceeded(minimum_rows=1)`；
- 非法 CIDR/布尔/预算/IPv4 文本和编译规模限制；
- 4096 条分散 `/32` 路由、4096 条 `/32` 授权、32 个出口和预算 4096 的规模
  用例，并抽查授权且有路由、授权但无路由、未授权但有路由三类点。

关键断言通过后，离线路径测试会打印实际行数、`fast`/`software` 来源和拒绝
类型，例如：

```text
[offload acceptance 1] budget_2_rejection=OffloadBudgetExceeded(minimum_rows=3); budget_3_actual_rows=3; ...
[offload acceptance 2] hole_.17_online_path=('software', 'sent', 'east'); hole_.17_offline_rejection=SoftwareUnavailable; ...
[offload scale] routes=4096 ranges=4096 egresses=32 budget=4096; actual_rows=4096; ...
```

正向案例使用真实临时回环 UDP socket，并由测试自行按收包 socket 和合成载荷
给出独立预期，不调用被测选路函数计算正确答案。并发案例使用 `Event`、
`Barrier` 和 2 秒有界 `join`，不依靠 `sleep` 猜测先后顺序。

关键断言通过后，测试会打印实际观察到的异常类型/原因、修订号以及在哪个接收
出口收到了哪些载荷，至少包含场景 1 和场景 2，例如：

```text
[scenario 1 observed] first rejection=RouteProtected(reason='route_protected', revision=1); ...
[scenario 2 observed] concurrent_dispatch=BatchBusy, concurrent_cancel=BatchBusy; ...
```

这些值来自当次异常、`revision` 和真实 socket 收包结果，而不是把测试预期直接
当作实测输出。
