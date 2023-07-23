import asyncio
import json


class Scheduler:
    def __init__(self, max_tasks=10):
        self.max_tasks = max_tasks
        self.tasks = []

    def add_task(self, task) -> None:
        if len(self.tasks) >= self.max_tasks:
            raise Exception("Scheduler is full. Cannot add more tasks.")
        self.tasks.append(task)

    async def execute_tasks(self) -> None:
        tasks_to_execute = [task for task in self.tasks if self.can_start_task(task)]

        while tasks_to_execute:
            tasks_in_progress = []
            for task in tasks_to_execute:
                tasks_in_progress.append(asyncio.create_task(task.execute()))

            await asyncio.gather(*tasks_in_progress)

            tasks_to_execute = [task for task in self.tasks if self.can_start_task(task)]

    def can_start_task(self, task) -> bool:
        if task.status == "completed":
            return False
        for dependency in task.dependencies:
            if dependency.status != "completed":
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
                    "dependencies": [dep.task_id for dep in task.dependencies],
                    "status": task.status,
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
        task_map = {task.task_id: task for task in self.tasks}

        for task_info in state.get("tasks", []):
            task_id = task_info["task_id"]
            if task_id in task_map:
                task = task_map[task_id]
                task.duration = task_info.get("duration")
                task.start_time = task_info.get("start_time")
                task.restarts = task_info.get("restarts")
                task.dependencies = [task_map[dep_id] for dep_id in task_info.get("dependencies", [])]
                task.status = task_info.get("status", "waiting")
