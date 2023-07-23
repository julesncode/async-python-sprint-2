import asyncio
import logging
import os
from pathlib import Path
import requests
from typing import Any, Optional

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


async def create_directory(directory_name: str) -> None:
    Path(directory_name).mkdir(exist_ok=True)
    print(f"Создана директория: {directory_name}")


async def create_file_and_write(filename: str, content: str) -> None:
    with open(filename, "w") as file:
        file.write(content)
    print(f"Создан файл {filename} и записано содержимое: {content}")


async def read_file(filename: str) -> None:
    print(f"Началась задача чтения файла...")
    with open(filename, "r") as file:
        content = file.read()
    print(f"Содержимое файла {filename}: {content}")


async def append_to_file(filename: str, additional_content: str) -> None:
    with open(filename, "a") as file:
        file.write(additional_content)
    print(f"Дополнительные данные добавлены в файл {filename}")


async def remove_directory(directory_name: str) -> None:
    os.rmdir(directory_name)
    print(f"Удалена директория: {directory_name}")


async def remove_file(filename: str) -> None:
    os.remove(filename)
    print(f"Удален файл: {filename}")


async def create_file(filename: str) -> None:
    with open(filename, "w") as file:
        file.write('')
    print(f"Создан пустой {filename}")


async def get_data_from_url(url: str) -> None:
    resp = requests.get(url)
    if resp.status_code == 200:
        data = resp.text
        data = data[0:100]
        logging.info(f"Задание получило данные: {data}")
    else:
        logging.warning(f"Задание не смогло получить данные. Код ошибки: {resp.status}")


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
        self.status = "waiting"
        self.func = func
        self.kwargs = kwargs

    async def execute(self) -> None:
        logging.info(f"Задание {self.task_id} начало выполняться.")
        self.status = "running"
        if self.start_time:
            await asyncio.sleep(self.start_time)
        for attempt in range(self.restarts + 1):
            try:
                await self.run()
                self.status = "completed"
                break
            except Exception as e:
                self.status = "failed"
                print(f"Задание {self.task_id} обвалилось на попытке номер {attempt + 1}: {e}")
        logging.info(f"Задание {self.task_id} завершилось со статусом: {self.status}")

    async def run(self) -> None:
        logging.info(f"Задание {self.task_id} начало выполнение функции.")
        if self.duration:
            await asyncio.sleep(self.duration)
        await self.func(**self.kwargs)
        logging.info(f"Задание {self.task_id} завершило выполнение функции.")
