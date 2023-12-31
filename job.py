import time
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
            max_restarts: int = 1,
            dependencies: Optional[list] = None,
            **kwargs: dict,
    ):
        self.task_id = task_id
        self.duration = duration
        self.start_time = start_time
        self.restarts = restarts
        self.max_restarts = max_restarts
        self.dependencies = dependencies or []
        self.status = JobStatus.WAITING
        self.func = func
        self.kwargs = kwargs

    def execute(self) -> None:
        logger.info(f"Задание {self.task_id} начало выполняться.")
        self.status = JobStatus.RUNNING
        if self.start_time:
            time.sleep(self.start_time)

        for attempt in range(self.max_restarts + 1):
            try:
                coroutine = self.run()
                while True:
                    try:
                        next(coroutine)
                    except StopIteration:
                        break
                self.status = JobStatus.COMPLETED
                break
            except Exception as e:
                self.status = JobStatus.FAILED
                logger.error(f"Задание {self.task_id} обвалилось на попытке номер {attempt + 1}: {e}")
        logger.info(f"Задание {self.task_id} завершилось со статусом: {self.status}")

    def run(self):
        logger.info(f"Задание {self.task_id} начало выполнение функции.")
        if self.duration:
            time.sleep(self.duration)
        yield from self.func(**self.kwargs)
        logger.info(f"Задание {self.task_id} завершило выполнение функции.")
