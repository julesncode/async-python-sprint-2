from job import *
from scheduler import Scheduler


async def main():
    scheduler = Scheduler(max_tasks=10)

    tasks_state_filename = "tasks_state.json"
    if os.path.isfile(tasks_state_filename):
        scheduler.load_state(tasks_state_filename)

    task1 = Job(task_id=1, duration=5, func=create_directory, directory_name="test_directory")
    task2 = Job(task_id=2, duration=3, dependencies=[task1], func=create_file_and_write,
                filename="test_directory/file1.txt", content="Проба пера 1")
    task3 = Job(task_id=3, duration=1, dependencies=[task1, task2], start_time=4, restarts=1,
                func=read_file, filename="test_directory/file1.txt")
    task4 = Job(task_id=4, duration=1, dependencies=[task1], start_time=4, restarts=1,
                func=create_file, filename="test_directory/file2.txt")
    task5 = Job(task_id=5, duration=3, dependencies=[task1, task2], func=append_to_file,
                filename="test_directory/file1.txt", additional_content="\nДобавляем содержимого, а то маловато")
    task6 = Job(task_id=6, duration=1, dependencies=[task1, task2, task3, task5], start_time=4, restarts=1,
                func=remove_file, filename="test_directory/file1.txt")
    task7 = Job(task_id=7, duration=1, dependencies=[task1, task4], start_time=4, restarts=1,
                func=remove_file, filename="test_directory/file2.txt")
    task8 = Job(task_id=8, duration=1, dependencies=[task6, task7], start_time=4, restarts=1,
                func=remove_directory, directory_name="test_directory")
    task9 = Job(task_id=9, func=get_data_from_url, url="https://www.google.com/")

    scheduler.add_task(task1)
    scheduler.add_task(task2)
    scheduler.add_task(task3)
    scheduler.add_task(task4)
    scheduler.add_task(task5)
    scheduler.add_task(task6)
    scheduler.add_task(task7)
    scheduler.add_task(task8)
    scheduler.add_task(task9)

    await scheduler.execute_tasks()
    await asyncio.sleep(1)
    scheduler.save_state(tasks_state_filename)


if __name__ == "__main__":
    asyncio.run(main())
