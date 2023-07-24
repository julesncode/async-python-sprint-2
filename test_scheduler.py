import json
import os
import shutil
import time
import unittest

from job import Job, JobStatus
from sample_tasks import (
    create_directory, create_file_and_write, read_file
)
from scheduler import Scheduler


class TestSchedulerAndJob(unittest.TestCase):

    def setUp(self):
        self.tasks_state_filename = "test_tasks_state.json"

    def tearDown(self):
        if os.path.isfile(self.tasks_state_filename):
            os.remove(self.tasks_state_filename)

    def test_scheduler_add_task(self):
        scheduler = Scheduler(max_tasks=2)
        task1 = Job(task_id=1, func=create_directory, directory_name="test_directory")
        scheduler.add_task(task1)
        self.assertEqual(len(scheduler.tasks), 1)

    def test_scheduler_execute_tasks(self):
        test_dir_name = "asfsdafasfas"
        scheduler = Scheduler(max_tasks=5)
        task1 = Job(task_id=1, duration=5, func=create_directory, directory_name=test_dir_name)
        task2 = Job(task_id=12, dependencies=[task1], func=create_file_and_write,
                    filename=f"{test_dir_name}/file1.txt", content="Test Content")
        task3 = Job(task_id=13, dependencies=[task2], func=read_file, filename=f"{test_dir_name}/file1.txt")
        task1.execute()
        scheduler.add_task(task2)
        scheduler.add_task(task3)
        scheduler.execute_tasks()

        # Add a small delay to allow tasks to complete
        time.sleep(2)

        self.assertTrue(os.path.exists(f"{test_dir_name}"))
        self.assertTrue(os.path.isfile(f"{test_dir_name}/file1.txt"))
        shutil.rmtree(test_dir_name, ignore_errors=True)

    def test_job_max_restarts(self):
        # This task function will raise an exception 3 times and then stop execution with proper task status
        def task_function():
            if task_function.attempts < 3:
                task_function.attempts += 1
                raise ValueError("Test error")
            return

        task_function.attempts = 0

        task = Job(task_id=1, func=task_function, max_restarts=3)
        task.execute()

        # Wait for the task to complete all attempts
        time.sleep(3)

        # The task should have failed status after reaching max_restarts
        self.assertEqual(task.status, JobStatus.FAILED)

    def test_scheduler_max_parallel_tasks(self):
        test_dir_name = "test_directory"
        scheduler = Scheduler(max_tasks=2)
        task1 = Job(task_id=1, duration=1, func=create_directory, directory_name=test_dir_name)
        task2 = Job(task_id=2, dependencies=[task1], func=create_file_and_write,
                    filename=f"{test_dir_name}/file1.txt", content="Test Content")
        task3 = Job(task_id=3, dependencies=[task2], func=read_file, filename=f"{test_dir_name}/file1.txt")

        scheduler.add_task(task1)
        scheduler.add_task(task2)

        error_status = ""
        try:
            scheduler.add_task(task3)
        except Exception as e:
            error_status = str(e)

        # Only 2 tasks can run, so adding task3 should raise specific error
        self.assertEqual(error_status, "Scheduler is full. Cannot add more tasks.")

    def test_scheduler_save_and_load_state(self):
        test_dir_name = "test_directory"
        scheduler = Scheduler(max_tasks=5)
        task1 = Job(task_id=1, duration=1, func=create_directory, directory_name=test_dir_name)
        task2 = Job(task_id=2, dependencies=[task1], func=create_file_and_write,
                    filename=f"{test_dir_name}/file1.txt", content="Test Content")
        task3 = Job(task_id=3, dependencies=[task2], func=read_file, filename=f"{test_dir_name}/file1.txt")

        scheduler.add_task(task1)
        scheduler.add_task(task2)
        scheduler.add_task(task3)

        scheduler.execute_tasks()

        # Wait for the tasks to complete
        while any(task.status == JobStatus.RUNNING for task in scheduler.tasks):
            time.sleep(0.1)

        # Save the state and create a new scheduler to load the state
        scheduler.save_state(self.tasks_state_filename)
        new_scheduler = Scheduler(max_tasks=5)
        new_scheduler.load_state(self.tasks_state_filename)
        # Check if the new scheduler will not continue already completed tasks
        self.assertEqual(len(new_scheduler.tasks), 0)

        # Check if the new scheduler can read info about tasks which remained not completed

        new_tasks = []

        with open(self.tasks_state_filename, "r") as file:
            content = json.load(file)
            for t in content['tasks']:
                t['status'] = JobStatus.WAITING.value
                new_tasks.append(t)

            new_content = {"max_tasks": content["max_tasks"],
                           "tasks": new_tasks}

        with open(self.tasks_state_filename, "w") as file:
            json.dump(new_content, file, indent=4)

        new_scheduler = Scheduler(max_tasks=5)
        new_scheduler.load_state(self.tasks_state_filename)

        self.assertEqual(len(new_scheduler.tasks), len(scheduler.tasks))
        for new_task, original_task in zip(new_scheduler.tasks, scheduler.tasks):
            self.assertEqual(new_task.task_id, original_task.task_id)
            self.assertEqual(new_task.start_time, original_task.start_time)
            self.assertEqual(new_task.status, JobStatus.WAITING.value)
            self.assertEqual(new_task.restarts, original_task.restarts)
            self.assertEqual(new_task.max_restarts, original_task.max_restarts)


if __name__ == "__main__":
    unittest.main()
