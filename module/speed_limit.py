"""全局下载限速。

所有下载任务共享同一个令牌桶, 由 Pyrogram 的下载 progress 回调
(module.download_stat.update_download_status) 按每次新增字节数扣配额。
配额计算在 asyncio.Lock 内串行完成, 等待在锁外并发执行, 因此并发任务数
不影响总速率: 全局吞吐 ≈ rate。

Web 配置中心通过 /set_speed_limit 调用 limiter.configure() 即可热更新,
不需要重启容器; 数值写入 config.yaml, 重启后由 Application.assign_config 恢复。
"""

import asyncio
import time


class GlobalSpeedLimiter:
    """负债式令牌桶: 配额可以短暂为负, 负值换算成后续任务的等待时间。"""

    def __init__(self):
        self._mbps = 0.0
        self._rate = 0.0  # bytes/s, 0 = 不限速
        self._tokens = 0.0
        self._last = time.monotonic()
        self._updated_at = time.time()
        self._total_bytes = 0
        self._total_wait = 0.0
        # Python 3.10+ 的 Lock 在首次 await 时才绑定事件循环, 这里可在模块导入时创建
        self._lock = asyncio.Lock()

    @property
    def mbps(self) -> float:
        return self._mbps

    def configure(self, mbps: float):
        """设置全局上限(MB/s)。0 表示解除。可在任意线程调用, 赋值是原子操作。"""
        value = max(float(mbps), 0.0)
        self._mbps = value
        self._rate = value * 1024 * 1024
        # 改速率时清空桶, 让新值立刻生效而不是等旧令牌耗尽
        self._tokens = 0.0
        self._last = time.monotonic()
        self._updated_at = time.time()

    def status(self) -> dict:
        return {
            "enabled": self._rate > 0,
            "mbps": self._mbps,
            "rate_bps": int(self._rate),
            "updated_at": self._updated_at,
            "total_bytes": self._total_bytes,
            "total_wait_seconds": round(self._total_wait, 2),
        }

    async def consume(self, nbytes: int):
        """按本次回调新增的字节数扣配额, 超出部分转成异步等待。

        Parameters
        ----------
        nbytes : int
            本次 progress 回调与上次回调之间新增的字节数
        """
        if self._rate <= 0 or nbytes <= 0:
            return
        async with self._lock:
            now = time.monotonic()
            rate = self._rate
            # 令牌最多累积 1 秒的量, 避免长时间空闲后瞬间大突发
            self._tokens = min(rate, self._tokens + (now - self._last) * rate)
            self._last = now
            self._tokens -= nbytes
            delay = (-self._tokens) / rate if self._tokens < 0 else 0.0
            self._total_bytes += nbytes
            self._total_wait += delay
        if delay > 0:
            await asyncio.sleep(delay)


# 进程内唯一实例, 被 download_stat / app / web 共同引用
limiter = GlobalSpeedLimiter()
