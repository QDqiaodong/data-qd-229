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

测试：`test_journal.py` 覆盖原单文件日志；`test_retained_journal.py` 覆盖
续读/确认语义、回收与回收到 `end`、两类重开结果、空间预算，以及在两个回调
点用真实子进程 `SIGKILL` 终止后重开的切点、确认位置、后缀与清理结果。

