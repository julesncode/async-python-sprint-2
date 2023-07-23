import asyncio
import os
import unittest

from job import Job, create_directory, create_file_and_write, read_file, append_to_file
from scheduler import Scheduler


class TestSchedulerAndJob(unittest.TestCase):
    def setUp(self):
        self.scheduler = Scheduler(max_tasks=10)
        self.tasks_state_filename = "test_tasks_state.json"

    def tearDown(self):
        if os.path.isfile(self.tasks_state_filename):
            os.remove(self.tasks_state_filename)

    async def test_scheduler_execute_tasks(self):
        task1 = Job(task_id=1, duration=1, func=create_directory, directory_name="test_directory")
        task2 = Job(task_id=2, duration=1, dependencies=[task1], func=create_file_and_write,
                    filename="test_directory/file1.txt", content="Test Content")
        task3 = Job(task_id=3, duration=1, dependencies=[task2], func=read_file, filename="test_directory/file1.txt")
        task4 = Job(task_id=4, duration=1, dependencies=[task2], func=append_to_file,
                    filename="test_directory/file1.txt", additional_content="Appended Content")

        self.scheduler.add_task(task1)
        self.scheduler.add_task(task2)
        self.scheduler.add_task(task3)
        self.scheduler.add_task(task4)

        await self.scheduler.execute_tasks()

        self.assertTrue(os.path.exists("test_directory"))
        self.assertTrue(os.path.isfile("test_directory/file1.txt"))
        with open("test_directory/file1.txt", "r") as file:
            content = file.read()
            self.assertEqual(content, "Test ContentAppended Content")

    async def test_scheduler_save_and_load_state(self):
        task1 = Job(task_id=1, func=create_directory, directory_name="test_directory")
        task2 = Job(task_id=2, dependencies=[task1], func=create_file_and_write,
                    filename="test_directory/file1.txt", content="Test Content")
        task3 = Job(task_id=3, dependencies=[task2], func=read_file, filename="test_directory/file1.txt")

        self.scheduler.add_task(task1)
        self.scheduler.add_task(task2)
        self.scheduler.add_task(task3)

        await self.scheduler.execute_tasks()
        await asyncio.sleep(1)
        self.scheduler.save_state(self.tasks_state_filename)

        new_scheduler = Scheduler(max_tasks=5)
        new_scheduler.load_state(self.tasks_state_filename)

        self.assertEqual(len(new_scheduler.tasks), 3)
        self.assertEqual(new_scheduler.tasks[0].status, "completed")
        self.assertEqual(new_scheduler.tasks[1].status, "completed")
        self.assertEqual(new_scheduler.tasks[2].status, "completed")


if __name__ == "__main__":
    unittest.main()
