"""Multiprocessing 启动策略：为 GPMQ worker 强制使用 spawn 语义。"""

from __future__ import annotations

import multiprocessing


def use_spawn(force: bool = True) -> None:
    """将 multiprocessing 启动方式设为 ``spawn``。

    必须在创建任何 multiprocessing 对象（Process / Event / Queue）之前调用，
    即在 ``Subscriber.run_forever()`` 之前。建议放在程序入口最前面。

    spawn 下每个 worker 都是全新 Python 解释器，**不继承**父进程内存，
    从而避免 fork 继承导致的脏全局态泄漏：handler 注册表、loguru 日志 fd、
    Redis 连接 socket、心跳线程持有的锁等。这也使 Linux 与 Windows 行为一致。

    Args:
        force: 为 True 时可重复调用而不抛 "context has already been set"。
    """
    multiprocessing.set_start_method("spawn", force=force)
