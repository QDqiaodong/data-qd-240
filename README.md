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

## 接收侧重整与有序交付（`receiving.py`）

`WindowReceiver(manifest, byte_budget)` 在接收器侧把跨数据报的测试样本重新
拼回完整字节，并按流、按序号有序交付给比对程序。它**只接收、不发送**：没有
socket、定时器或后台线程，不探测、不补发、不访问外网；`seal` 是本地调度决定，
不是网络控制包。发送、选路、批次承诺和离线镜像的行为均不变。

### 数据报格式与 `encode_slice`

```python
from receiving import encode_slice
datagram = encode_slice(number, offset, data)
# 前 8 字节：大端 uint32 样本序号、大端 uint32 从零开始的字节偏移；其后为非空 bytes
```

- 载荷不含目标 IP、接收器名或任何控制指令，接收器名由调用方按实际收包 socket
  提供；
- 整报不超过原发送入口的 4096 字节，`offset + len(data) <= 65536`；
- 拒绝非法类型（`bool` 不算整数、`data` 必须是非空 `bytes`）、零序号和超界字段。

### 清单、流与构造校验

`manifest` 是非空映射：1–8 个非空字符串接收器名，每个名字映射到 1–128 个样本
长度（1–65536 的非布尔整数），样本序号从 1 开始。`byte_budget` 是 0–1048576
的非布尔整数，整个实例共享。构造时完整校验并固定清单；实例支持单线程串行调用，
不做持久化。不同流的同序号样本不共享任何字节或交付状态。

### `feed(receiver, datagram) -> (status, events)`

先校验再改动：未知流、非法报文、清单中不存在的序号或超出该样本长度的片段一律
抛 `ValueError` 且不改变任何流。返回标记：

| 状态 | 含义 |
| --- | --- |
| `"accepted"` | 片段纳入；可能同时返回有序交付事件 |
| `"duplicate"` | 合法重复片，不增加任何覆盖，无事件 |
| `"conflict"` | 重叠字节与已保留字节不一致；整片拒绝，连它带来的新字节也不保留 |
| `"late"` | 序号已终结（已交付或已封缺口），不重开样本 |
| `"pressure"` | 见下方压力策略，不纳入来片、无事件 |

收齐判断只看每个字节位置的实际覆盖，不看片段数、累计片长，也不做零填充。

### 交付与 `seal(receiver, through)`

每条流从序号 1 起按序交付，每号恰好一次：

- 收齐的样本在轮到它时交付 `("message", receiver, number, data)`；前序未终结时
  即使自己完整也继续保留；一次 `feed` 可在同一次调用里链式交付随后所有连续完整
  样本；
- `seal(receiver, through)`（`0 <= through <= 本流清单长度`，越界/非法类型/未知流
  抛 `ValueError` 且无副作用）声明截至 `through` 不再等缺片：已终结序号不重发；
  范围内完整样本按原字节交付 `message`，不完整（含完全没收到的）丢掉部分字节并
  交付 `("gap", receiver, number, "sealed")`；随后连续完整的后继即使超过界限也
  立即交付。被封缺口的序号永久关闭，之后同号片段一律 `late`；其他流不受影响。

### 共用缓冲预算与选定的压力策略：保留前序

只读属性 `buffered_bytes` 只统计尚未终结样本**已保留的不同字节**：包括缺片中的
字节和排在前序之后的完整样本；不含空洞、重复字节和已交付事件字节，也没有任何
不入账的副本或文件转存。

本实现只实现题目允许的两种策略之一——**保留前序**：

- 来片纳入后，若按正常交付规则链式交付完仍会超过 `byte_budget`，立即返回
  `("pressure", ())`，不保留来片任何字节，原有覆盖和交付状态完全不变；
- 若纳入后随即的正常交付就能腾出足够空间，则必须纳入（例如来片正好补全队头，
  队头若干完整样本在同一次调用里交付释放空间）；
- 不会为腾空间牺牲任何已保留样本，也不能跨流销账或跳过自己流上的完整样本。
  调度方只能接受这次未纳入，或在空间释放后用手头的合成样本重新提交同一接口。

### 最小回环示例

`demo_receive.py` 沿用原有 `Forwarder.send` 和 `demo.receiver` 风格的临时回环
UDP 接收器：先真实收包，再把**实际收到的原始报文**交给 `WindowReceiver.feed`，
打印每一步的返回标记、交付事件和缓冲用量：

```sh
python3 demo_receive.py
# east sample-2-first  status=accepted  events=() buffered=3
# east sample-1-tail   status=accepted  events=() buffered=6
# west complete        status=accepted  events=(('message', 'west', 1, b'west'),) buffered=6
# east sample-1-head   status=accepted  events=(('message', 'east', 1, b'abcde'),
#                                              ('message', 'east', 2, b'XYZ')) buffered=0
```

#### 可选录制与重开续读

不传参数时行为与上面完全一致。加 `--spool PATH` 创建一份本地交付记录，并把
**本次真实收包后由接收器实际生成的每个非空组**按原顺序提交；没有事件的步骤不
提交。`--read PATH` 是另一次运行：它按原配置重开该记录，从比较器的已确认游标
续读，然后确认、回收并再读一次，用来展示录制方退出后结果仍在：

```sh
python3 demo_receive.py --spool /tmp/drill.spool
# ... 原四行输出 ...
# delivery record created: /tmp/drill.spool (reader='comparator', capacity=64,
#   groups=2, highest_delivery=3)

python3 demo_receive.py --read /tmp/drill.spool
# record reopened: groups=2 highest_delivery=3 comparator_cursor=0 usage=12
#   page=[(1, ('message', 'west', 1, b'west')),
#         (2, ('message', 'east', 1, b'abcde'))]
# comparator confirmed through 2
# record reclaimed through 2, usage now 3
# comparator reread after reclamation: [(3, ('message', 'east', 2, b'XYZ'))]
```

`--spool` 拒绝覆盖已存在的路径；要重开请用 `--read`。两个参数不能同时给出。
记录的对象只是接收器已经产出的事件，演示不会因此改变收发、重组或默认输出。

## 可重开的本地交付记录（`delivery_spool.py`）

`DeliverySpool` 把一个演练中接收器产出的非空事件组记录到本地文件，让两套比对
程序各自按需读取、各自确认，并允许录制方和读者在进程退出后原样重开。它只处理
**已经生成的事件**（`message` / `gap`），不接触 UDP 数据报，不恢复未完成的重组，
不发送、不探测、不访问外网；只用 Python 3.9+ 标准库。

### 创建与重开

```python
from delivery_spool import DeliverySpool

spool = DeliverySpool.create("/tmp/drill.spool", ["cmpA", "cmpB"],
                             capacity=64)          # 新建（路径已存在则抛 RecordExists）
...
spool = DeliverySpool.open("/tmp/drill.spool")     # 重开：沿用原读者名与容量，保留进度
```

- 创建时确定非空且互异的读者名与正整数容量（`bool` 不算整数）；重开时只读回
  原配置，重新 `create` 一个已存在的路径会被拒绝，绝不静默覆盖。
- 评价按串行方式使用：一次只有一个进程打开记录。不提供多进程并发写，也不模拟
  断电；写入以整份记录的原子替换提交，因此重开看到的是某次完整提交或更早的
  完整提交。
- 只读诊断：`readers`、`capacity`、`usage`/`usage_bytes`、`retained_events`、
  `highest_delivery`、`reclaimed_through`、`committed_groups`、
  `next_group_number`、`cursor(reader)`、`stored_receipt(group_number)`。

### 提交整组：`submit(group_number, events) -> DeliveryReceipt`

`events` 是接收器原样的 4 元组序列，例如
`(("gap", "s", 1, "sealed"), ("message", "s", 2, b"XYZ"))`。提交规则：

- 每组必须非空；空序列抛 `EmptyGroup`，不会提交，也不消耗任何编号。
- 组号由调用者提供，从 1 连续递增；正常前进时只能是 `next_group_number`。
- 记录给事件另发从 1 起的连续**交付号**，顺序按各次提交、组内按原顺序；回执
  `DeliveryReceipt(group_number, first_delivery, last_delivery, event_count)`
  指出这组的首尾交付号。
- 只暴露整组结果：容量不足时抛 `CapacityExceeded`，写入在提交前失败时抛
  `SpoolWriteError`；两者都不改变记录、不消耗组号或交付号，读者看不到半组。
- **重放最近一次已提交组**：用 `group_number == 最近已提交组号` 且内容相同
  （按规范内容摘要判定）重试，直接返回原回执，不写记录——即使该组已被回收，
  或当前空间放不下同样的数据。
- 同号异内容抛 `GroupConflict`；早于最近已提交组号抛 `StaleGroup`；向前跳号抛
  `GroupGap`；三者都可与容量不足区分，且不修改记录。提交前失败的组可以用原
  组号再试。

### 容量与用量

`usage` 按**仍留存的事件**计：完整样本计载荷字节数，缺口计 1；读者游标、回执
元数据和底层文件页开销不计。确认或回收尚未成功时，已可删的事件仍占容量——只有
回收提交成功才归还额度，`reclaim()` 返回的 `ReclaimResult`
（`reclaimed_through`、`freed_events`、`freed_bytes`、`usage`）报告实际归还量。
容量不足时由调用者决定何时重试；记录不会跳过读者、丢弃已发布事件或阻塞等待
读者确认。

### 各读各的：`read` / `confirm` / `reclaim`

```python
page = spool.read("cmpA", limit=2)   # [(交付号, 原事件), ...]
spool.confirm("cmpA", 3)             # 累计确认：不再需要 3 号及以前
result = spool.reclaim()             # 删除所有读者都已确认的公共前缀
```

- `read(reader, limit)` 从该读者自己的已确认交付号之后分页返回，`limit` 必须是
  正整数（`bool` 不算）。读取不等于确认，可重复读。
- `confirm(reader, through)` 报累计确认号；不得超过已提交的最高交付号（否则
  `InvalidAck`），对有效读者重复确认旧号是无副作用的成功并返回当前游标。
- 未知读者抛 `UnknownReader`（`LookupError` 子类），非法页大小抛
  `InvalidPageSize`。
- 重开后恢复各自确认进度；没确认且未回收的结果仍可读，允许比对程序自己重复
  处理——本记录不宣称对外部比对操作恰好一次。

**选定的确认/回收路线：独立提交**（题目允许的两条路线之一）。`confirm` 单独
持久化游标；`reclaim` 是显式且独立的提交，把公共确认前缀整体删除。若回收写失败，
确认仍然有效，未归还的容量继续占用，调用者只需重试 `reclaim()` 而不必重交确认。
公共前缀的删除按整份记录的原子替换完成，不会只删一部分就宣布成功；没有可回收
前缀时抛 `NothingToReclaim`，与容量不足、与回收写失败都可区分。

### 错误一览

| 情况 | 异常 | 说明 |
| --- | --- | --- |
| 创建/读取参数非法 | `ConfigurationError`(`ValueError`) | 含容量、读者名、页大小、确认号 |
| 新建已存在路径 | `RecordExists` | `ConfigurationError` 子类 |
| 记录文件损坏/版本不符 | `CorruptRecord`(`ValueError`) | 重开时校验结构 |
| 未知读者 | `UnknownReader`(`LookupError`) | |
| 组内容形状非法 / 空组 | `InvalidEvent` / `EmptyGroup` | |
| 组号不对 | `GroupNumberError`(`ValueError`) | 含 `GroupConflict`/`StaleGroup`/`GroupGap` |
| 容量不足 | `CapacityExceeded` | **不是** `ValueError`，不消耗编号 |
| 无可回收前缀 | `NothingToReclaim` | 与写失败分开 |
| 提交前写失败 | `SpoolWriteError` | 记录保持原提交 |

### 恢复语义（重开时如何解释现状）

重开后从文件中读回：读者名与容量、已提交组的连续编号与交付号区间、仍留存的
事件（含完整字节或缺口原因）、各读者游标、`highest_delivery`（含已回收区间）、
`reclaimed_through`。由此可解释：每个读者下一次 `read` 从何处开始、哪些交付号
已被确认、哪些字节仍占容量。写入模型只有两种可见状态——上一次成功提交的记录或
更早的完整记录；回收与确认各自的提交是原子的，不存在“删了一半就对外成功”的
中间态。

### 与接收演示接通

`demo_receive.py --spool` 只把每步**实际收包后由 `WindowReceiver` 生成的非空
事件组**提交给记录（空组不提交），`--read` 再重开续读。发送、批次、离线镜像与
接收重组接口及其回归测试均保持不变。

## 测试与复现

```sh
python3 -m unittest -v
python3 -m unittest -v test_forwarding.py
python3 -m unittest -v test_batch_forwarding.py
python3 -m unittest -v test_offload.py
python3 -m unittest -v test_receiving.py
python3 -m unittest -v test_delivery_spool.py
python3 demo.py
python3 demo_receive.py
python3 demo_receive.py --spool /tmp/drill.spool && python3 demo_receive.py --read /tmp/drill.spool
```

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

`test_receiving.py` 覆盖接收侧验收，且所有预期字节都是测试内的合成字面量，不
用被测代码生成答案：

- 场景 1：同一份“先收样本 1 中段、再收齐样本 2”的输入跑两条对照路线——不封口时
  补齐前序后按序得到 `abcdef`、`XYZ`；封口界限从 0 改到 1 时先得第 1 号
  `gap` 再得第 2 号完整字节，且同一补片之后一律 `late`，缺口不会变回消息。
  另核对一致重叠/带新字节的冲突重叠/非法越界片对覆盖和预算的影响，以及完全未
  收到的序号随封口产生缺口；
- 场景 2：预算 8、已占 7 字节时来片再要 2 字节——保留前序策略返回 `pressure`、
  占用仍是 7；补齐队头的调用一次交付样本 1、2 后，重新提交的样本 3 才交付；
  第二条流在预算凑满时不能从第一条流销账或跳过自己；并覆盖预算临界值与零预算；
- 场景 3：通过真实 `Forwarder.send` 把编码片段发到两个临时 IPv4 回环接收器，
  只把各自实际 `recv` 到的原始报文喂给接收模块；同序号不同字节的两条流不能互
  拼，校验交付事件中的接收器名、顺序和准确字节，结束后两个 socket 均无余包；
- 单元覆盖：`encode_slice` 类型/零序号/超界校验、清单与预算构造校验、`feed`
  未知流/坏报文/越界校验且无副作用、`duplicate`/`conflict`（新字节随整片丢弃）/
  `late`、乱序链式交付、流之间互不共享状态、`seal` 非法参数无副作用。

关键断言通过后，场景 1、2（以及场景 3）会打印当次实际观察到的交付/缺口顺序、
返回标记和缓冲用量，例如：

```text
[scenario 1 observed] branch=seal_through_1 seal_events=[gap:1:sealed, message:2:b'XYZ']; later_slice_statuses=['late', 'late', 'late']; buffered_bytes=0
[scenario 2 observed] policy=retain_predecessor pressure_status=pressure (stream b, retained=7, no cross-stream write-off); ...
[scenario 3 observed] real_loopback b_delivery=[message:b:1:b'0123']; a_delivery=[message:a:1:b'efgh', message:a:2:b'SECD']; ...
```

这些输出来自真实调用返回值、事件元组和 `buffered_bytes`，不是预先写好的结果。

`test_delivery_spool.py` 覆盖交付记录的三条验收。预期交付号、用量和分页内容
都是测试中独立写出的字面量；持久化失败夹具直接替换真实的
`delivery_spool._atomic_replace`（提交边界）与 `_after_commit`（提交后回调），
而不是用非法参数模拟写失败：

- 场景 1：用真实 `WindowReceiver` 先收齐样本 2，再 `seal` 封掉样本 1，得到
  “缺口 + 完整样本”整组。先在提交边界注入失败，重开后读者看不到半组、组号与
  交付号都未消耗；用同一组号重写，交付号 1–2 连续。随后在提交成功后让回调抛错
  模拟回执丢失，重开后重复提交同组内容得到原号码区间且只读到一份；改动一字节的
  同号重试抛 `GroupConflict`。全部确认并回收后再次重开、重试同一组号仍返回原回
  执（3–3），记录不复活；更早组号抛 `StaleGroup`；
- 场景 2：容量 8、两名读者，写入 4 字节完整样本 + 缺口 + 3 字节完整样本（用量
  恰为 8）。甲确认至 3、乙仅读不确认时回收抛 `NothingToReclaim`，录制方提交下一
  组 4 字节样本被 `CapacityExceeded(requested=4, available=0)` 拒绝；仅把乙确认
  推进到 1 后回收，用量由 8 降到 4，原被拒组以交付号 4 提交成功。重开后乙读到
  2、3、4，甲只读到 4；
- 场景 3：从同一初始进度出发，乙确认到 1 后令回收的删除路径在提交前失败——重开
  后乙的确认仍在（游标 1），从 2 开始读，用量仍为 8，原被拒组仍被拒；只重试回收
  即成功归还 4 字节，随后可写入原被拒组。失败抛 `SpoolWriteError`，与
  `NothingToReclaim` 明确区分，也不能靠放宽容量绕过；
- 另有创建/重开配置保留、未知读者、非法页大小/确认号、组内容形状校验、缺口计 1
  与完整样本计载荷、公共前缀精确回收，以及接收演示录制模式只提交实际产出的非空组
  并可在另一次打开后续读的覆盖。

关键断言通过后，测试打印当次实际观察到的拒绝类型、确认后读到的交付号和用量，
例如：

```text
[delivery scenario 1 observed] interrupted_write=SpoolWriteError (replace attempts=1); committed_range_after_retry=1-2; lost_receipt_replay=3-3; ...
[delivery scenario 2 observed] empty_reclaim=NothingToReclaim; rejected_submit=CapacityExceeded(requested=4, available=0); usage_before=8; usage_after_reclaim=4; ... b_reads=[2, 3, 4]; a_reads=[4]
[delivery scenario 3 observed] empty_reclaim=NothingToReclaim; failed_reclaim=SpoolWriteError; ... cursor_b_after_reopen=1; reclaimed_through_after_failure=0; usage_after_failure=8; b_reads_after_reopen=[2, 3]; ...
```

这些值来自当次异常属性、`cursor`/`read` 与 `usage` 的实际返回，而不是把固定预期
当作实测打印。
