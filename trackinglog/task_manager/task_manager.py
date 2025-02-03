import os
import json
import shutil
from os.path import join as pjoin
from datetime import datetime, timedelta
from typing import Optional, Union, List, Callable

class TaskFolderStruct:
    def __init__(self, task_folder_path):
        self._paths = {}  # Store paths in a private dictionary
        folder_structure = {
            "temp": "tmp",
            "cache": "tmp/cache",
            "var": "var",
            "result": "result",
        }

        for attr, relpath in folder_structure.items():
            fd_path = os.path.abspath(pjoin(task_folder_path, relpath))
            os.makedirs(fd_path, exist_ok=True)
            self._paths[attr] = fd_path

    # Property methods to provide read-only access
    @property
    def temp(self):
        return self._paths["temp"]

    @property
    def cache(self):
        return self._paths["cache"]

    @property
    def var(self):
        return self._paths["var"]

    @property
    def result(self):
        return self._paths["result"]
    
    def __repr__(self) -> str:
        return f"You can access sub-folder paths through ['."+ "']; ['.".join(self._paths.keys()) + "']"



class TaskMgtAgent:
    __slots__ = ['_task_folder_path', '_curr_task_folder_path', '_task_expiration_date', '_task_num_limit', '_task_folder_format', '_folder_path_config']

    def __init__(self, task_folder_path: str, data: Union[dict, Callable]) -> None:
        self._task_folder_path = None
        self._task_expiration_date = None
        self._task_num_limit = None
        self._task_folder_format = None

        # Use getattr for compact initialization
        if isinstance(data, dict):
            task_expiration_date = data.get('task_expiration_date', None)
            task_num_limit = data.get('task_num_limit', 10)
            task_folder_format = data.get('task_folder_format', "%y%m%d_%H%M%S")
            resume_task = data.get('resume_task', False)
        else:
            assert hasattr(data, "task_expiration_date") \
                   and hasattr(data, "task_num_limit") and hasattr(data, "task_folder_format"), \
                   "Invalid data type for task config"

            task_expiration_date = getattr(data, "task_expiration_date", None)
            task_num_limit = getattr(data, "task_num_limit", 10)
            task_folder_format = getattr(data, "task_folder_format", "%y%m%d_%H%M%S")
            resume_task = getattr(data, "resume_task", False)

        self.setup(task_folder_path, task_expiration_date, task_num_limit, task_folder_format, resume_task)


    def _clean_old_tasks(self, resume_task) -> None:
        """Deletes folders older than expiration date and keeps only the latest task_num_limit folders."""
        
        # Get all folders in the task directory
        folders = [f for f in os.listdir(self._task_folder_path) if os.path.isdir(os.path.join(self._task_folder_path, f))]
        assert not resume_task or resume_task in folders, f"Can not find task {folders} in history"

        # Parse timestamps from folder names and filter valid folders
        valid_folders = []
        for folder in folders:
            folder_path = os.path.join(self._task_folder_path, folder)
            folder_timestamp = self._extract_date_from_folder(folder)

            if folder_timestamp:
                valid_folders.append((folder_timestamp, folder, folder_path))

        # Sort folders by timestamp in **descending order** (newest first)
        valid_folders.sort(reverse=True, key=lambda x: x[0])

        # Step 1: **Keep only the latest `task_num_limit` folders, delete the rest**
        if self._task_num_limit is not None:
            folders_to_delete = valid_folders[self._task_num_limit:]
            valid_folders = valid_folders[:self._task_num_limit]
            if resume_task and any(resume_task == tup[1] for tup in folders_to_delete):
                folders_to_delete.remove(resume_task)
                valid_folders.append(resume_task)
            for _, folder, folder_path in folders_to_delete:
                shutil.rmtree(folder_path)
                print(f"Deleted old task folder: {folder}")
        
        # Step 2: **Delete folders older than expiration date**
        if self._task_expiration_date:
            try:
                expiration_days = int(self._task_expiration_date)
                cutoff_date = datetime.now() - timedelta(days=expiration_days)

                expired_folders = [f for f in valid_folders if f[0] < cutoff_date and f[1] != resume_task]
                for _, folder, folder_path in expired_folders:
                    shutil.rmtree(folder_path)
                    print(f"Deleted expired task folder: {folder}")

            except ValueError:
                print(f"Invalid expiration date format: {self._task_expiration_date}")

        if resume_task:
            return resume_task  # Resume existing task
        else:
            # Create a new task folder with the current timestamp
            new_task_name = datetime.now().strftime(self._task_folder_format)
            os.makedirs(pjoin(self._task_folder_path, new_task_name), exist_ok=True)
            print(f"Created new task folder: {new_task_name}")
            return new_task_name  # Return new task folder name

    def _extract_date_from_folder(self, folder_name: str) -> Optional[datetime]:
        """Extracts the date from folder name using the task_folder_format."""
        try:
            # Assuming timestamp is at the end of the folder name
            return datetime.strptime(folder_name, self._task_folder_format)
        except ValueError as e:
            print(f"Invalid expiration date format: {e}")
            return None
        
    def setup(self, task_folder_path: str, task_expiration_date: Optional[str], task_num_limit: int, task_folder_format: str, resume_task: bool) -> None:
        """Setup the task configuration."""
        config_path = os.path.join(task_folder_path, "__task_config.json")
        if os.path.exists(config_path):
            with open(config_path, "r") as f:
                old_config=json.load(f)
            if old_config["task_folder_format"]!=task_folder_format:
                print(f"Warning: Conflict for task_folder_format, revert to old task_folder_format {old_config.get('task_expiration_date')}")   
                task_folder_format = old_config["task_folder_format"]
            if old_config.get("task_expiration_date", task_expiration_date)!=task_expiration_date:
                print(f"Warning: Reset task_expiration_date from {old_config.get('task_expiration_date')} to {task_expiration_date}")
            if old_config.get("task_num_limit", task_num_limit)!=task_num_limit:
                print(f"Warning: Reset task_num_limit from {old_config.get('task_num_limit')} to {task_num_limit}")


        self._task_folder_path = task_folder_path
        self._task_expiration_date = task_expiration_date
        self._task_num_limit = task_num_limit
        self._task_folder_format = task_folder_format

        current_task_ts = self._clean_old_tasks(resume_task)
        self._curr_task_folder_path = pjoin(task_folder_path, current_task_ts)
        
        config_data = {
            "task_expiration_date": task_expiration_date,
            "task_num_limit": task_num_limit,
            "task_folder_format": task_folder_format
        }

        with open(config_path, "w") as f:
            json.dump(config_data, f, indent=4)

        self._folder_path_config = TaskFolderStruct(self._curr_task_folder_path)

    @property
    def task_folder_path(self) -> str:
        return self._task_folder_path

    @task_folder_path.setter
    def task_folder_path(self, value: str) -> None:
        self._task_folder_path = value

    @property
    def task_expiration_date(self) -> Optional[str]:
        return self._task_expiration_date

    @task_expiration_date.setter
    def task_expiration_date(self, value: Optional[str]) -> None:
        self._task_expiration_date = value

    @property
    def task_num_limit(self) -> int:
        return self._task_num_limit

    @task_num_limit.setter
    def task_num_limit(self, value: int) -> None:
        self._task_num_limit = value

    @property
    def task_folder_format(self) -> str:
        return self._task_folder_format

    @task_folder_format.setter
    def task_folder_format(self, value: str) -> None:
        self._task_folder_format = value

    @property
    def folder_path_config(self) -> TaskFolderStruct:
        return self._folder_path_config

    @property
    def curr_task_folder_path(self) -> str:
        return self._curr_task_folder_path


    def __repr__(self) -> str:
        return (f"TaskConfig(task_folder_path={self._task_folder_path}, task_expiration_date={self._task_expiration_date}, "
                f"task_num_limit={self._task_num_limit}, task_folder_format={self._task_folder_format})")
