# data-qd-240 · IPv4 目的路由与 UDP 出口

用户态测试转发器接收 IPv4 目的地址和字节载荷；按最长的匹配前缀选择命名出口，
或在该前缀配置丢弃时不发送。不匹配返回 `unrouted`。命中命名出口时通过真实
UDP socket 发送，不会把发送成功等同于接收确认。`replace(routes)` 完整校验后
更换整张表；重复、非规范前缀和未知出口拒绝，不改变当前表。

Python 3.9+，标准库。目的地址是调用方给出的元数据，不解析 IP 包；
所有出口必须是 IPv4 回环地址。调用方按顺序使用同一实例，以上下文管理 socket。
不改宿主路由、不做真实网络转发、健康探测、广播或持久化。
演示使用自制载荷和两个临时回环 UDP 接收端。

```sh
python3 -m unittest -v
python3 demo.py
```

预期输出 `specific -> east: b'sample'`、`more specific -> drop`、
`after replacement -> west: b'sample'`。
