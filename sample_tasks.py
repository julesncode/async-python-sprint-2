import os
from pathlib import Path

import requests

from scheduler_logger import logger


def create_directory(directory_name: str) -> None:
    Path(directory_name).mkdir(exist_ok=True)
    logger.info(f"Создана директория: {directory_name}")
    yield


def create_file_and_write(filename: str, content: str) -> None:
    with open(filename, "w") as file:
        file.write(content)
    logger.info(f"Создан файл {filename} и записано содержимое: {content}")
    yield


def read_file(filename: str) -> None:
    logger.info(f"Началась задача чтения файла...")
    with open(filename, "r") as file:
        content = file.read()
    logger.info(f"Содержимое файла {filename}: {content}")
    yield


def append_to_file(filename: str, additional_content: str) -> None:
    with open(filename, "a") as file:
        file.write(additional_content)
    logger.info(f"Дополнительные данные добавлены в файл {filename}")
    yield


def remove_directory(directory_name: str) -> None:
    os.rmdir(directory_name)
    logger.info(f"Удалена директория: {directory_name}")
    yield


def remove_file(filename: str) -> None:
    os.remove(filename)
    logger.info(f"Удален файл: {filename}")
    yield


def create_file(filename: str) -> None:
    with open(filename, "w") as file:
        file.write('')
    logger.info(f"Создан пустой {filename}")
    yield


def get_data_from_url(url: str) -> None:
    resp = requests.get(url)
    if resp.status_code == 200:
        data = resp.text
        data = data[0:100]
        logger.info(f"Задание получило данные: {data}")
    else:
        logger.warning(f"Задание не смогло получить данные. Код ошибки: {resp.status_code}")
    yield
