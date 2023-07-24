import asyncio
from enum import Enum
from typing import Any, Optional

from scheduler_logger import logger


class JobStatus(str, Enum):
    COMPLETED = 'completed'
    RUNNING = 'running'
    WAITING = 'waiting'
    FAILED = 'failed'


class Job:
    def __init__(
            self,
            task_id: int,
            func: Any,
            duration: Optional[float] = None,
            start_time: Optional[float] = None,
            restarts: int = 0,
            dependencies: Optional[list] = None,
            **kwargs: dict,
    ):
        self.task_id = task_id
        self.duration = duration
        self.start_time = start_time
        self.restarts = restarts
        self.dependencies = dependencies or []
        self.status = JobStatus.WAITING
        self.func = func
        self.kwargs = kwargs

    async def execute(self) -> None:
        logger.info(f"Задание {self.task_id} начало выполняться.")
        self.status = JobStatus.RUNNING
        if self.start_time:
            await asyncio.sleep(self.start_time)
        for attempt in range(self.restarts + 1):
            try:
                await self.run()
                self.status = JobStatus.COMPLETED
                break
            except Exception as e:
                self.status = JobStatus.FAILED
                logger.error(f"Задание {self.task_id} обвалилось на попытке номер {attempt + 1}: {e}")
        logger.info(f"Задание {self.task_id} завершилось со статусом: {self.status}")

    async def run(self) -> None:
        logger.info(f"Задание {self.task_id} начало выполнение функции.")
        if self.duration:
            await asyncio.sleep(self.duration)
        await self.func(**self.kwargs)
        logger.info(f"Задание {self.task_id} завершило выполнение функции.")
