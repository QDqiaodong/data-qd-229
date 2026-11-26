# 可恢复的本地追加日志

进程将小型二进制事件写入 `Journal(path)`，获得帧的字节偏移；
消费者从保存的偏移 `read(offset, limit=...)`，用每条记录的 `next_offset` 续读。
追加在完成文件 flush/fsync 后返回。重启验证完整前缀，只截尾部不完整的帧；
已完整但校验失败的帧报错，保留原文件。重启后仍可继续按旧游标读取。

Python 3.9+，标准库。自制演示在临时目录构造真实的截断尾部，再恢复与追加：

```sh
python3 -m venv .venv
.venv/bin/python -m pip install --no-index -r requirements.txt
.venv/bin/python -m unittest -v
.venv/bin/python demo.py
```

预期 `discarded tail bytes: 20` 和 `replayed: [(4, 'started'), (23, 'finished')]`。

格式：4 字节魔数 AJ1+换行；每帧为三个大端 uint32：长度、负载 CRC32、
前两个字段的 CRC32，随后是原始负载（最多 1 MiB）。第一个帧偏移为 4。
仅单进程、单活动句柄，不并发写也不支持外部修改。打开时线性扫描并保存偏移索引。
CRC32 用于检测意外损坏，不提供鉴权；fsync 不额外同步父目录，不保证新建文件
目录项在断电后保留。失败的追加需要重开确认，不含去重、分段或日志保留策略。

# 带消费者游标回收的 RetainedJournal

`RetainedJournal(directory, *, prune_hook=None)` 在独立目录上提供同样的 AJ1
帧编码与 `append`/`read` 接口，并允许在线物理回收所有消费者都处理完的前缀。
它仍限定同一时刻一个进程持有一个活动句柄（目录内 `lock` 上的 `flock`）。

```python
from journal import RetainedJournal, ExpiredOffset

with RetainedJournal("./run/log-a") as log:
    first = log.append(b"event-1")
    log.register("worker-1")          # 从当前 floor 开始留存，返回确认位置
    # ... 消费者读到 end 后，确认 next_offset 之前的帧都已处理
    log.ack("worker-1", log.end)     # 同值重试无副作用，只能前进到帧起点/end
    log.prune()                      # floor 推进到最小确认位置并释放旧字节
    try:
        log.read(first)
    except ExpiredOffset:            # ValueError 子类：游标已低于 floor
        pass
```

- 逻辑偏移在整个目录生命周期内单调、不复用：首帧仍在 4，`floor` 是最早可读
  偏移，`end` 是下次追加偏移；回收只推进 `floor`，幸存帧的 `offset`、
  `next_offset` 与负载不变。回收到 `end` 后下一帧仍从旧 `end` 追加。
- 消费者：`register(name)`（1–32 位 ASCII 字母/数字/`_`/`-`，至多 16 个，
  重复注册返回原确认位置）、`checkpoint(name)`、`ack(name, next_offset)`、
  `drop(name) -> bool`。注册/确认/注销都不立即移动 `floor`，状态随清单持久化，
  重开后仍有效；未知名称的查询/确认抛 `KeyError`，非法名称或超上限抛
  `ValueError`。
- 回收：`prune() -> int` 把 `floor` 精确推进到当前所有消费者确认位置的最小值；
  无消费者或最小值未前进时不推进。回收以 `manifest.json` 的原子替换为唯一提交
  点，因此重开只能看到完整的旧切点或完整的新切点。非空回收会在“已暂存、未
  发布”与“已发布、未清理”两个时刻各调用一次 `prune_hook("before_publish" /
  "after_publish")`，回调中直接结束进程也能在重开后保持一致并清理临时产物。
- 重开：保留完整前缀，仅截去当前追加端不完整的尾帧，其字节数计入
  `recovered_bytes`；可读范围内完整但校验失败的帧抛 `Corruption`，不会被当作
  尾帧截掉。
- 空间：可读帧至多 65,536 个、编码合计至多 64 MiB、单帧负载至多 1 MiB，逻辑
  `end` 不超过有符号 64 位整数上限。数据、暂存与状态全部位于该目录内，不使用
  符号链接。

目录布局（实现细节，可能调整）：

```
directory/
  lock                 # 单活动句柄的 flock 文件（零字节）
  manifest.json        # floor、消费者确认位置、段文件名与各段逻辑起点
  seg-NNNNNNNNNNN.aj   # 数据段：4 字节 AJ1 魔数 + 若干 AJ1 帧
  *.tmp                # 回收暂存文件，打开时自动清理
```

每段的逻辑起点持久化在清单里，重写切点所在段时只复制幸存后缀，回收期间额外
占用不超过一个段（约 256 KiB）量级的空间。

# 乱序接收与无缺口回放

`ReplicaJournal(directory, *, source_id, max_pending_bytes)` 在独立接收目录中
保存中继送达的现有 `Record`，支持上下文管理；只使用标准库。`source_id` 必须是
1–64 个 ASCII 字母、数字、`_` 或 `-`，`max_pending_bytes` 是 0–64 MiB（含）的
整数。首次打开会写入 `replica.json`，以后重开必须提供完全相同的两个参数，否则
抛 `ValueError` 且不会重置目录。

```python
from journal import Journal, ReplicaJournal

with Journal("./run/source.aj") as source:
    records = source.read(cursor, limit=8)

with ReplicaJournal(
    "./run/replica",
    source_id="source-42",
    max_pending_bytes=2 * 1024 * 1024,
) as replica:
    new_end = replica.ingest(records)  # 非空 list/tuple，最多 256 条/4 MiB
    print(replica.end, replica.pending_bytes)

    # 只返回从 offset=4 开始、已经连续的前缀；到达 end 时返回 []。
    rows = replica.read(cursor, limit=4)
    if rows:
        cursor = rows[-1].next_offset
```

- `end` 初值为 `4`，是从 4 起的最长连续已接收前缀末端。远处的帧会持久保存并
  计入 `pending_bytes`，但不能通过 `read` 绕过缺口；补齐缺口后，已暂存的后段
  立即可按原游标读取，无需重传。
- 只有同起点、同末端且负载逐字节相同的帧算重复；其他区间重叠抛
  `ReplicaConflict`（`ValueError` 子类）。去重、接通连续前缀后仍在缺口之后的
  帧才按 `12 + len(data)` 计预算；超预算抛 `ReplicaBackpressure`（也是
  `ValueError` 子类）。冲突检查先于预算检查，两种拒绝都不会留下本批新帧。
- 每帧使用单独的 `frame-<offset>.aj` 文件保存 AJ1 编码，不用大文件偏移空洞
  表示未收到的区间，因此真实块占用不会为四十多 MiB 缺口写入实心填充。

测试：`test_journal.py` 覆盖原单文件日志；`test_retained_journal.py` 覆盖
续读/确认语义、回收与回收到 `end`、两类重开结果、空间预算，以及在两个回调
点用真实子进程 `SIGKILL` 终止后重开的切点、确认位置、后缀与清理结果；
`test_replica_journal.py` 覆盖 A–H 乱序/重复/冲突/预算/重开场景，并在 Linux
子进程的 40 MiB `RLIMIT_AS` 中验证 48 个近 1 MiB 帧、块占用和分批回放。

# 按设备顺序派发的 LeasedWorkJournal

`LeasedWorkJournal(directory, *, hook=None)` 在独立目录上实现带租约的任务
队列：同一设备（`key`）的任务严格按提交顺序派发，一台设备的工作进程失联只
挡住它自己，不影响其他设备。任务正文逐帧写入 `directory/events` 下的
`RetainedJournal`，它是唯一的权威任务日志；租约与完成状态另存
`directory/leases.json`，全部状态都在本目录内，只使用标准库，支持上下文
管理，同一时刻仍只允许一个活动句柄。

```python
from journal import LeasedWorkJournal

with LeasedWorkJournal("./run/work") as queue:
    first = queue.submit("device-A", b"op-1")   # -> 帧字节偏移，即任务编号
    queue.submit("device-A", b"op-2")
    queue.submit("device-B", b"op-3")
    dispatch = queue.claim(now=100, ttl=30)     # 选可领设备头中偏移最小者
    try:
        ...                                       # 调用方自行执行设备操作
    finally:
        if queue.finish(dispatch.receipt, now=105):
            queue.reclaim()                      # 回收连续已完成前缀的正文
```

- `submit(key, data) -> int`：`key` 为区分大小写的 1–32 个 ASCII 字母、数字、
  `_` 或 `-`；`data` 为至多 1,000,000 字节的 `bytes`，可为空。沿用底层
  65,536 帧、64 MiB 可读编码与有符号 64 位末端边界，已完成但尚未回收的帧
  同样占用配额；任何参数/配额拒绝都不会多写一帧。
- `claim(now, ttl) -> Dispatch | None`：从各设备尚未完成的首条任务中选出当前
  可领且偏移最小者；某设备首条任务持有有效租约时，该设备后续任务不可领取，
  其他设备照常。`Dispatch` 含 `offset`、`key`、`data`、`receipt`、`deadline`
  五个只读属性，`deadline = now + ttl`，有效区间不含 `deadline`。`now` 为
  0 至 2**62−1 的真整数（布尔值不算），`ttl` 为 1 至 86,400 的真整数；库不
  读系统时钟、不睡眠等待过期，由调用方保证同目录时钟不倒退。每次成功领取都
  签发队列内不复用的新非空 `receipt`；到期重领同一偏移也会换发凭据。
- `finish(receipt, now) -> bool`：只接受对应任务当前且未到期的凭据，成功则
  永久完成该任务并返回 `True`。超时、已被替换、已使用或不属于当前领取的凭据
  一律返回 `False`，且不会完成任务、顶替其他凭据或释放日志；凭据类型错误/
  为空、`now` 非法抛 `ValueError` 且不留状态变化。
- `reclaim() -> int`：只真实回收连续已完成的前缀并返回回收后的 `floor`；后面
  的设备任务即使已完成，也不能越过一条仍未完成的老任务被回收，阻塞任务完成
  后再次调用才会连同后续连续已完成帧一起释放。`finish` 本身不做物理回收。
  重开不改变偏移、不让已完成任务重新可领；回收到空后仍按旧 `end` 继续追加。
  只读的 `floor`、`end` 分别反映权威日志的最早可读偏移与追加末端。
- 租约、截止时间、设备阻挡随 `leases.json` 持久化：成功领取后关闭重开，原凭据
  仍可在有效期内确认，不会因重开另发凭据。迟到的旧凭据回报在租约超时并被替换
  后只会得到 `False`，无法把当前任务从队列里删掉。
- `hook` 可选：提交在底层 append 已 fsync 之后、返回编号之前回调
  `hook("after_payload_sync")`；完成在完成决定已持久化之后、返回 `True` 之前
  回调 `hook("after_finish_commit")`。回调中直接结束进程也保持一致：前者重开后
  任务恰好保留一份且可领，后者重开后任务不复活并可参与下一次前缀回收。
- 大积压：打开时以小批量流式扫描重建只含设备键与偏移的内存索引，不缓存任务
  正文。因此在开好的队列上，即使有超过 50 MiB 的同设备积压、绝大多数帧被该
  设备首条任务的有效租约挡住，单次 `claim` 也只读取最终命中的那一帧（实测
  `/proc/self/io` 的 `rchar` 增量为数百字节量级，上限 2 MiB）；准备积压、
  重开加领取可在 48 MiB 地址空间限制的新 Python 进程中完成。

目录布局（实现细节，可能调整）：

```
directory/
  events/           权威 RetainedJournal：每任务一个 AJ1 帧，偏移即任务编号
  leases.json       凭据计数、当前有效租约（偏移 -> [凭据, 截止时间]）、完成偏移
  leases.json.tmp   leases.json 的原子替换暂存文件
```

任务帧正文编码为 1 字节键长度、ASCII 键、随后原始 `data`；外部设备操作是否
恰好执行一次不由本队列保证，重复领取/确认的幂等由调用方结合凭据处理。

测试：`test_leased_work_journal.py` 覆盖 A/A/B 完整派发-重开-超时重领-回收
流程（并用原 `RetainedJournal` 核对真实切点与过期游标）、空任务与回收到空后
继续提交、凭据与时钟参数校验、配额拒绝与回收恢复、两个回调点的真实子进程
`SIGKILL` 重开一致性，以及 48 MiB `RLIMIT_AS` 子进程中 54 个近 1 MiB 帧积压
下 `claim` 的读取量与正确性。

