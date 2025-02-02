import os
from os.path import join as pjoin
from typing import Optional, Union, Callable

class EmailCredential:
    __slots__ = ['_username', '_password', '_root_emails_folder']

    def __init__(self, data: Union[dict, Callable], default_emails_folder: str = 'emails') -> None:
        self._username = None
        self._password = None
        self._root_emails_folder = None

        if isinstance(data, dict):
            self.setup(
                email_root_folder=data.get('root_emails_folder', default_emails_folder),
                username=data.get('username'),
                password=data.get('password')
            )
        else:
            # Assuming 'data' is an object with attributes 'username' and 'password'
            assert hasattr(data, 'username') and hasattr(data, 'password'), "Invalid data type for email credential"
            self.setup(
                email_root_folder=default_emails_folder if getattr(data, 'root_emails_folder', None) is None else data.root_emails_folder,
                username=data.username,
                password=data.password
            )
    
    def setup(self, email_root_folder: str, username: Optional[str] = None, password: Optional[str] = None) -> None:
        """
        Setup the email credential.
        Parameters:
            email_root_folder (str): The folder where emails are stored.
            username (str): The email username.
            password (str): The email password.
        """
        self.root_emails_folder = email_root_folder
        self.username = username if username is not None else self._username
        self.password = password if password is not None else self._password

    @property
    def username(self) -> str:
        return self._username

    @property
    def password(self) -> str:
        return self._password

    @property
    def root_emails_folder(self) -> str:
        return self._root_emails_folder

    @username.setter
    def username(self, value: str) -> None:
        self._username = value

    @password.setter
    def password(self, value: str) -> None:
        self._password = value

    @root_emails_folder.setter
    def root_emails_folder(self, value: str) -> None:
        self._root_emails_folder = value

    def __repr__(self) -> str:
        return f"EmailCredential(username={self._username}, password=***hidden***)"
    
class LogConfig:
    __slots__ = ['_root_log_path', '_cache_log_path', '_cache_log_num_limit', '_cache_log_day_limit']

    def __init__(self, data: Union[dict, Callable], default_log_path: str = 'logs') -> None:
        # Initialize default values
        self._root_log_path = None
        self._cache_log_path = None
        self._cache_log_num_limit = None
        self._cache_log_day_limit = None
        
        if isinstance(data, dict):
            self.setup(
                root_log_path=data.get('root_log_path', default_log_path),
                cache_log_path=data.get('cache_log_path'),
                cache_log_num_limit=data.get('cache_log_num_limit'),
                cache_log_day_limit=data.get('cache_log_day_limit')
            )
        else:
            # Assuming 'data' is an object with necessary attributes
            assert hasattr(data, "root_log_path") and hasattr(data, "cache_log_path") and hasattr(data, "cache_log_num_limit") and hasattr(data, "cache_log_day_limit"), "Invalid data type for log config"
            self.setup(
                root_log_path=data.root_log_path if data.root_log_path is not None else default_log_path,
                cache_log_path=data.cache_log_path,
                cache_log_num_limit=data.cache_log_num_limit,
                cache_log_day_limit=data.cache_log_day_limit
            )

    def setup(self, root_log_path: str = 'logs', cache_log_path: Optional[str] = None, cache_log_num_limit: Optional[int] = None, cache_log_day_limit: Optional[int] = None) -> None:
        self.root_log_path = root_log_path
        self.cache_log_path = cache_log_path if cache_log_path is not None else pjoin(root_log_path, "cache")
        self.cache_log_num_limit = cache_log_num_limit if cache_log_num_limit is not None else 100
        self.cache_log_day_limit = cache_log_day_limit if cache_log_day_limit is not None else 7

    @property
    def root_log_path(self) -> str:
        return self._root_log_path

    @property
    def cache_log_path(self) -> str:
        return self._cache_log_path

    @property
    def cache_log_num_limit(self) -> int:
        return self._cache_log_num_limit

    @property
    def cache_log_day_limit(self) -> int:
        return self._cache_log_day_limit

    @root_log_path.setter
    def root_log_path(self, value: str) -> None:
        self._root_log_path = value

    @cache_log_path.setter
    def cache_log_path(self, value: str) -> None:
        self._cache_log_path = value

    @cache_log_num_limit.setter
    def cache_log_num_limit(self, value: int) -> None:
        self._cache_log_num_limit = value

    @cache_log_day_limit.setter
    def cache_log_day_limit(self, value: int) -> None:
        self._cache_log_day_limit = value
    
    def __repr__(self) -> str:
        return (f"LogConfig(root_log_path={self._root_log_path}, cache_log_path={self._cache_log_path}, "
                f"cache_log_num_limit={self._cache_log_num_limit}, cache_log_day_limit={self._cache_log_day_limit})")
    

class LockConfig:
    __slots__ = ['_lock_folder_path']

    def __init__(self, data: Union[dict, Callable], default_lock_path: str = 'locks') -> None:
        self._lock_folder_path = None
        if isinstance(data, dict):
            self.setup(
                lock_folder_path=data.get('lock_folder_path', default_lock_path)
            )
        else:
            assert hasattr(data, "lock_folder_path"), "Invalid data type for lock config"
            self.setup(
                lock_folder_path=default_lock_path if data.lock_folder_path is None else data.lock_folder_path
            )

    def setup(self, lock_folder_path: str = 'locks') -> None:
        """
        Setup the lock configuration.
        Parameters:
            lock_folder_path (str): Path for lock file.
        """
        self.lock_folder_path = lock_folder_path

    @property
    def lock_folder_path(self) -> str:
        return self._lock_folder_path

    @lock_folder_path.setter
    def lock_folder_path(self, value: str) -> None:
        self._lock_folder_path = value

    def __repr__(self) -> str:
        return f"LockConfig(lock_folder_path={self._lock_folder_path})"

class ParameterConfig:
    __slots__ = ['_root_task_path', '_log_config', '_email_credential', '_lock_config']

    def __init__(self) -> None:
        """
        Create a new ParameterConfig object with placholder values.
        """
        self._root_task_path = None
        self._log_config = None
        self._email_credential = None
        self._lock_config = None

    @property
    def root_task_path(self) -> str:
        return self._root_task_path

    @property
    def email_credential(self) -> Optional[EmailCredential]:
        return self._email_credential

    @property
    def log_config(self) -> Optional[LogConfig]:
        return self._log_config
    
    @property
    def lock_config(self) -> Optional[LockConfig]:
        return self.lock_config

    @root_task_path.setter
    def root_task_path(self, value: str) -> None:
        assert ParameterConfig.validate_and_create_path(value),  f"Invalid Cache Log Path Given: {value}"
        self._root_task_path = value
    
    @email_credential.setter
    def email_credential(self, value: Optional[Union[Callable, dict]]) -> None:
        self._email_credential = EmailCredential(value, pjoin(self._root_task_path, "emails"))

    @log_config.setter
    def log_config(self, value: Optional[Union[Callable, dict]]) -> None:
        self._log_config = LogConfig(value, pjoin(self._root_task_path, "logs"))

    @lock_config.setter
    def lock_config(self, value: Optional[Union[Callable, dict]]) -> None:
        self._lock_config = LockConfig(value, pjoin(self._root_task_path, "locks"))
    
    def setup(self, root_task_path: str = 'logs', log_config: Optional[Union[Callable, dict]] = None, email_credential: Optional[Union[Callable, dict]] = None, lock_config: Optional[Union[Callable, dict]] = None) -> None:
        """
        Setup the root logging path and initialize cache logging configuration.
        Parameters:
            root_task_path (str): The root path where log files will be stored.
            cache_log_limit (int): The limit on the number of cache log files.
            cache_log_days (int): The number of days to keep cache log files.
        """
        self.root_task_path = root_task_path
        self.log_config = log_config  if log_config is not None else {}
        self.email_credential = email_credential if email_credential is not None else {}
        self.lock_config = lock_config if lock_config is not None else {}
        


    @staticmethod
    def validate_and_create_path(root_task_path: str) -> bool:
        """
        Validate and create the root log path if necessary.
        Parameters:
            root_task_path (str): The path to validate and create if it does not exist.
        Returns:
            bool: True if the path is valid and accessible, False otherwise.
        """
        if not root_task_path:
            return False
        try:
            os.makedirs(root_task_path, exist_ok=True)
        except Exception as e:
            print("Error during creating the folder.", e)
            return False
        return True

    def _convert_to_email_credential(self, credential: Optional[Union[EmailCredential, dict]]) -> Optional[EmailCredential]:
        if isinstance(credential, dict):
            return EmailCredential(**credential)
        elif isinstance(credential, EmailCredential):
            return credential
        elif credential is None:
            return None
        else:
            raise ValueError("Invalid type for email credential")

    def __repr__(self) -> str:
        return f"LogConfig(root_task_path={self._root_task_path}, log_config={self._log_config}, email_credential={self._email_credential})"