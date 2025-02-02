import os
from os.path import join as pjoin
from typing import Optional, Union, Callable

class EmailAgent:
    # __slots__ = ['username', 'password', 'root_emails_folder', 'setup']

    def __init__(self, data: Union[dict, Callable], default_emails_folder: str = 'emails') -> None:
        self.username = None
        self.password = None
        self.root_emails_folder = None

        if isinstance(data, dict):
            self.setup(
                email_root_folder=data.get('root_emails_folder', default_emails_folder),
                username=data.get('username'),
                password=data.get('password')
            )
        else:
            # Assuming 'data' is an object with attributes 'username' and 'password'
            assert hasattr(data, 'username') and hasattr(data, 'password') and hasattr(data, 'root_emails_folder'), "Invalid data type for email credential"
            self.setup(
                email_root_folder=default_emails_folder if data.root_emails_folder is None else data.root_emails_folder,
                username=data.username,
                password=data.password
            )
    
    def setup(self, email_root_folder: str, username: Optional[str] = None, password: Optional[str] = None,) -> None:
        """
        Setup the email credential.
        Parameters:
            username (str): The email username.
            password (str): The email password.
        """
        self.root_emails_folder = email_root_folder
        self.username = username if username is not None else self.username
        self.password = password if password is not None else self.password

    @property
    def username(self) -> str:
        return self.username

    @property
    def password(self) -> str:
        return self.password

    @property
    def root_emails_folder(self) -> str:
        return self.root_emails_folder

    @username.setter
    def username(self, value: str) -> None:
        self.username = value

    @password.setter
    def password(self, value: str) -> None:
        self.password = value

    @root_emails_folder.setter
    def root_emails_folder(self, value: str) -> None:
        self.root_emails_folder = value

    def __repr__(self) -> str:
        return f"EmailCredential(username={self.username}, password=***hidden***)"
