import json
from collections import deque

from job import Job, JobStatus


class Scheduler:
    def __init__(self, max_tasks: int = 10):
        self.max_tasks = max_tasks
        self.tasks = deque()
        self.ready_tasks = deque()

    def add_task(self, task) -> None:
        if len(self.tasks) >= self.max_tasks:
            raise Exception("Scheduler is full. Cannot add more tasks.")
        self.tasks.append(task)

    def execute_tasks(self) -> None:
        self.ready_tasks.extend(task for task in self.tasks if self.can_start_task(task))

        while self.ready_tasks:
            tasks_in_progress = []
            while len(tasks_in_progress) < self.max_tasks and self.ready_tasks:
                task = self.ready_tasks.popleft()
                tasks_in_progress.append(task)

            self.execute_tasks_in_parallel(tasks_in_progress)

            self.ready_tasks.extend(task for task in self.tasks if self.can_start_task(task))

    def execute_single_task(self, task) -> None:
        task.status = JobStatus.RUNNING
        task.execute()

    def execute_tasks_in_parallel(self, tasks) -> None:
        for task in tasks:
            if task.status == JobStatus.WAITING:
                self.execute_single_task(task)

    def can_start_task(self, task: Job) -> bool:
        if task.status == JobStatus.COMPLETED:
            return False
        for dependency in task.dependencies:
            if dependency.status != JobStatus.COMPLETED:
                return False
        return True

    def save_state(self, file_path: str) -> None:
        state = {
            "max_tasks": self.max_tasks,
            "tasks": [
                {
                    "task_id": task.task_id,
                    "duration": task.duration,
                    "start_time": task.start_time,
                    "restarts": task.restarts,
                    "max_restarts": task.max_restarts,  # Add max_restarts to the saved state
                    "dependencies": [dep.task_id for dep in task.dependencies],
                    "status": task.status.value
                }
                for task in self.tasks
            ],
        }
        with open(file_path, "w") as file:
            json.dump(state, file, indent=4)

    def load_state(self, file_path: str) -> None:
        with open(file_path, "r") as file:
            state = json.load(file)

        self.max_tasks = state.get("max_tasks", self.max_tasks)
        task_map = {}

        for task_info in state.get("tasks", []):
            task_id = task_info["task_id"]
            if task_info["status"] != JobStatus.COMPLETED:
                task = Job(
                    task_id=task_info["task_id"],
                    duration=task_info.get("duration"),
                    start_time=task_info.get("start_time"),
                    restarts=task_info.get("restarts"),
                    max_restarts=task_info.get("max_restarts"),
                    dependencies=[],
                    func=None,
                    kwargs={},
                )
                task_map[task_id] = task

        for task_info in state.get("tasks", []):
            if task_info["status"] != JobStatus.COMPLETED:
                task_id = task_info["task_id"]
                task = task_map[task_id]
                task.dependencies = [task_map[dep_id] for dep_id in task_info.get("dependencies", [])]
                task.func = task_info.get("func")  # Get the task function directly from the loaded state
                task.kwargs = task_info.get("kwargs", {})

                self.tasks.append(task)

