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
