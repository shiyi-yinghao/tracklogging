import os
import io
import sys
import time
import copy
import types
import psutil
import atexit
import logging
import inspect
import datetime
import traceback
import pandas as pd
from contextlib import redirect_stdout
from line_profiler import LineProfiler
from collections import defaultdict as ddict
from functools import wraps, update_wrapper, singledispatch
from typing import Optional, Union, Any, Type, Callable, Tuple, Dict

from trackinglog.parameter_config import ParameterConfig
from trackinglog.email_manager import EmailAgent
from trackinglog.task_manager import TaskMgtAgent

class LogManager:
    """
    LogManager is a singleton class for managing and creating loggers across the application.
    """
    _instance = None
    
    def __new__(cls: Type['LogManager']) -> 'LogManager':
        """Create a new LogManager instance or return the existing one."""
        if not cls._instance:
            cls._instance = super(LogManager, cls).__new__(cls)
            cls._instance.config = ParameterConfig()
        return cls._instance
        
    def __init__(self) -> None:
        """Initialize the LogManager with an empty logger dictionary."""
        self.logger_dict={}

    def setup(self, *args, **kwargs):
        """Delegate the setup to the config object."""
        self.config.setup(*args, **kwargs)       
        
    def setup_check(func: Callable) -> Callable:
        """
        Decorator to ensure the log manager is properly set up before creating loggers.
        Parameters:
            func (Callable): The function to wrap with the setup check.
        Returns:
            Callable: The wrapped function.
        """
        def decorator(self, *args, **kwargs):
            if not hasattr(self, 'config') or not self.config.log_config.root_log_path:
                raise ValueError("Root log path must be set before using loggers.")
            return func(self, *args, **kwargs)
        return decorator
       
    @staticmethod
    def get_log_string(*args: Any, **kwargs: Any) -> str:
        """
        Convert logging arguments into a formatted string.
        Parameters:
            args: Arguments to be logged.
            kwargs: Keyword arguments controlling the formatting of the log string.
        Returns:
            str: The formatted log string.
        """
        modified_args = []
        for arg in args:
            if isinstance(arg, pd.DataFrame):
                # Convert DataFrame to string with added lines
                spliter="\n--------------------------------------\n"
                df_str =  spliter + arg.to_string(max_rows=kwargs.get("max_rows", 10), max_cols=kwargs.get("max_cols", 10), max_colwidth=kwargs.get("max_colwidth", 35), show_dimensions=True) + spliter
                modified_args.append(df_str)
            elif isinstance(arg, dict):
                spliter="\n- - - - - - - - - - - - - - - - - - - \n"
                rows = []
                for k, v in arg.items():
                    rows.append({'key': k, 'value': v})

                df_str = spliter + pd.DataFrame(rows).to_string(max_rows=kwargs.get("max_rows", 10), max_cols=kwargs.get("max_cols", 10), max_colwidth=kwargs.get("max_colwidth", 35), show_dimensions=True) + spliter
                modified_args.append(df_str)
            elif isinstance(arg, float):
                modified_args.append("{:.6f}".format(arg).rstrip('0'))
            else:
                modified_args.append(str(arg))

        msg = ' '.join(modified_args)
        return msg

    def cache_log_cleaner(self) -> None:
        """
        Clean up old log files based on the given limits for file number and age.
        """
        cache_log_dir = self.config.log_config.cache_log_path
        cache_log_limit = self.config.log_config.cache_log_num_limit
        cache_log_days = self.config.log_config.cache_log_day_limit

        # Ensure the log directory exists
        if not os.path.exists(cache_log_dir):
            return
        
        files = [os.path.join(cache_log_dir, f) for f in os.listdir(cache_log_dir) if os.path.isfile(os.path.join(cache_log_dir, f))]
        files_to_delete = []

        # If cache_log_limit is set, sort by file name (assuming file names are indicative of their creation or modification sequence) and delete excess files
        if cache_log_limit is not None:
            files_sorted_by_name = sorted(files, key=lambda x: os.path.basename(x), reverse=True)
            files_to_delete = files_sorted_by_name[cache_log_limit:]
            
        # If cache_log_days is set, delete files older than the specified number of days
        if cache_log_days is not None:
            cutoff_date = datetime.datetime.now() - datetime.timedelta(days=cache_log_days)
            for file in files:
                file_mod_time = datetime.datetime.fromtimestamp(os.path.getmtime(file))
                if file_mod_time < cutoff_date:
                    files_to_delete.append(file)

        for file in files_to_delete:
            os.remove(file)

    def create_cache_log(self) -> None:
        """
        Set up cache logging by cleaning old logs and creating a new cache logger.
        """
        _cache_log_key = "_CACHE"
        if _cache_log_key not in self.logger_dict:
            self.cache_log_cleaner()
            self.create_logger(_cache_log_key, folderpath=self.config.log_config.cache_log_path)
        return _cache_log_key

    @setup_check
    def create_logger(self, logname: str, filename: Optional[str] = None, folderpath: Optional[str] = None, log_level: int = logging.DEBUG, timestamp: bool = True, formart_align: bool = True) -> None:
        """
        Create a new logger with specified configurations.
        Parameters:
            logname (str): Name of the logger.
            filename (str, optional): Name of the log file.
            folderpath (str, optional): Path where the log file will be stored.
            log_level (int): Logging level.
            timestamp (bool): Whether to append timestamp to filename.
            formart_align (bool): Whether to align the format.
        Returns:
            None: Logger is configured and stored in the logger dictionary.
        """
        if not folderpath:
            folderpath = self.config.log_config.root_log_path
        if not filename:
            filename = logname
        else:
            filename = os.path.splitext(filename)[0]
        
        time_stamp=datetime.datetime.now().strftime("%y%m%d_%H%M%S")

        if timestamp:
            filename = f"{filename}_{time_stamp}.log"
        else:
            filename = f"{filename}.log"
        
        full_log_path = os.path.join(folderpath, filename)
        os.makedirs(folderpath, exist_ok=True)
        logger = logging.getLogger(logname)
        handler = logging.FileHandler(full_log_path)
        if formart_align:
            formatter = logging.Formatter('%(asctime)s-%(levelname)-8s-%(caller_func_name)-10s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        else:
            formatter = logging.Formatter('%(asctime)s-%(levelname)s-%(caller_func_name)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(log_level)

        def split_kwargs(**kwargs: Any) -> Tuple[Dict[str, Any], Dict[str, Any]]:
            log_kwargs = {key[5:]: value for key, value in kwargs.items() if key.startswith('_log_')}
            if "verbose" in kwargs:
                log_kwargs["verbose"]=kwargs["verbose"]
            other_kwargs = {key: value for key, value in kwargs.items() if not key.startswith('_log_') and key!="verbose"}
            return log_kwargs, other_kwargs
    
        def print_and_log(log_method: Callable, default_verbose: bool = True) -> Callable:
            @wraps(log_method)
            def wrapper(*args: Any, **kwargs: Any) -> Any:
                log_kwargs, remaining_kwargs = split_kwargs(**kwargs)
                system_msg=log_kwargs.get("system_msg", None)
                func_name = inspect.stack()[1].function if system_msg is None else system_msg
                msg = LogManager.get_log_string(*args, **log_kwargs)
                log_method(msg, extra={'caller_func_name': func_name}, **remaining_kwargs)
                if log_kwargs.get("verbose", default_verbose):
                    print(msg)
                if log_kwargs.get("notify", False):
                    # Place holder for email notification/kafka message sending
                    pass 
            return wrapper

        # Decorate existing logger methods
        logging_methods = ['info', 'debug', 'warning', 'error', 'critical']
        for method in logging_methods:
            setattr(logger, f'p{method}', print_and_log(getattr(logger, method), default_verbose=True))
            setattr(logger, method, print_and_log(getattr(logger, method), default_verbose=False))
        
        
        # Placeholder for debugging msg rewrite after inherit merchanism
        # logger._debugging_msg_dict = ddict(list)
        logger._debugging_msg = []
        setattr(logger, 'debugging_msg', lambda msg: logger._debugging_msg.append(msg))
        setattr(logger, 'folder', copy.deepcopy(self.config.task_config.folder_path_config))
        self.logger_dict[logname] = logger
        
        atexit.register(LogManager.close_log, logger, handler) 
    
    @setup_check
    def get_logger(self, logname: str, filename: Optional[str] = None, folderpath: Optional[str] = None, log_level: int = logging.DEBUG) -> logging.Logger:
        """
        Retrieve an existing logger or create a new one if it does not exist.
        Parameters:
            logname (str): Name of the logger.
            filename (str, optional): Name of the log file.
            folderpath (str, optional): Path where the log file will be stored.
            log_level (int): Logging level.
        Returns:
            logging.Logger: The requested logger.
        """
        if logname is None:
            logname = self.create_cache_log()
        # elif logname=="_INHERIT":
        #     self.create_cache_log(self, cache_log_limit: Optional[int], cache_log_days: Optional[int]) -> None:
        elif logname not in self.logger_dict:
            self.create_logger(logname, filename, folderpath, log_level)
        
        logger = self.logger_dict[logname]
        return logger

    @staticmethod
    def close_log(logger: logging.Logger, file_handler: logging.Handler) -> None:
        """
        Close the file handler of a logger and remove it from the logger.
        Parameters:
            logger (logging.Logger): The logger whose file handler should be closed.
            file_handler (logging.FileHandler): The file handler to close.
        """
        try:
            file_handler.close()
            logger.removeHandler(file_handler)
        except Exception:
            pass

        

    @setup_check    
    def get_log(self, logname: str, filename: Optional[str] = None, folderpath: Optional[str] = None, log_level: int = logging.DEBUG, verbose: int = 0, enable_profiling: Optional[str] = "function", print2log: bool = False) -> Callable:
        """
        Get a logger configured for function profiling, error handling, and optional output capture.
        Parameters:
            logname (str): Name of the logger.
            filename (str, optional): Filename for log output.
            folderpath (str, optional): Path to store log files.
            log_level (int): Log level to use.
            verbose (int): Verbosity level for log output.
            enable_profiling (str): Profiling type ("function", "line", or None).
            print2log (bool): If True, print outputs are captured and logged.
        Returns:
            Callable: A decorator to apply logging and profiling to functions or methods.
        """
        logger = self.get_logger(logname, filename=filename, folderpath=folderpath, log_level=log_level)

        def trace_error_msg() -> str:
            """
            Extracts and formats the traceback of the last exception.
            Returns:
                str: A formatted string of the traceback excluding entries that are part of the LogManager itself.
            """
            traceback_str = traceback.format_exc()
            lines = traceback_str.split('\n')
            filtered_lines = [line for i, line in enumerate(lines) if "log_manager" not in line \
                    and (i == 0 or "log_manager" not in lines[i - 1])]
            return '\n'.join(filtered_lines)
        
        class CapturePrints:
            """Context manager to capture print statements and optionally redirect them to logs."""
            def __init__(self) -> None:
                self._original_stdout = None
                self._stringio = None
                self.exception = None

            def __enter__(self) -> io.StringIO:
                self._original_stdout = sys.stdout  # Keep track of the original stdout
                self._stringio = io.StringIO()      # Create a StringIO buffer to capture output
                sys.stdout = self._stringio         # Replace the standard output
                return self._stringio

            def __exit__(self, exc_type: Optional[Type[BaseException]], exc_value: Optional[BaseException], traceback_info: Optional[Any]) -> Optional[bool]:
                sys.stdout = self._original_stdout  # Restore the original stdout
                if exc_type is not None:
                    # Capture the exception details
                    self.exception = (exc_type, exc_value, traceback_info)
                    return False  # Propagate the exception after logging it
                return True
            
        def manage_profiling(func: Callable, args: tuple, kwargs: dict, profiling_type: Optional[str] = None, print2log: bool = False) -> Any:
            """Manage profiling and log capturing based on settings."""
            if profiling_type == "line":
                profiler = LineProfiler()
                profiler.add_function(func)
                profiler.enable()
            elif profiling_type in ["function", "func"]:
                start_time = time.time()
                process = psutil.Process()
                cpu_before = process.cpu_percent(interval=None)
                memory_before = process.memory_info().rss
            
            if print2log:
                with CapturePrints() as captured:
                    result = func(*args, **kwargs)
                    captured_print = captured.getvalue()
                    logger.pinfo(captured_print, verbose=verbose, _log_system_msg=func.__name__)
            else:
                result = func(*args, **kwargs)

            if profiling_type == "line":
                profiler.disable()
                profiling_info = io.StringIO()
                profiler.print_stats(stream=profiling_info)
                profiling_info = profiling_info.getvalue()
                logger.info("LineProfiler Stats:\n"+profiling_info, _log_system_msg="<LOG_MANAGER>")

            elif profiling_type in ["function", "func"]:
                cpu_after = process.cpu_percent(interval=None)
                memory_after = process.memory_info().rss
                end_time = time.time()
                time_taken = end_time - start_time     
                logger.info(f"Time Taken: ** {func.__name__} ** took {time_taken//3600:.0f} hr {time_taken//60:.0f} min {time_taken % 60:.2f} sec", _log_system_msg="<LOG_MANAGER>")
                logger.info(f"Resource usage: CPU {cpu_after - cpu_before}%, Memory {((memory_after - memory_before) / (1024 * 1024)):.2f} MB", _log_system_msg="<LOG_MANAGER>")
            return result

        @singledispatch
        def decorator(obj: Any) -> Callable:
            raise NotImplementedError("Unsupported type")

        @decorator.register
        def _(cls: type) -> type:
            original_init = cls.__init__

            @wraps(cls.__init__)
            def wrapped_init(self, *args: Any, **kwargs: Any) -> None:
                original_init(self, *args, **kwargs)
                self.log = logger
            
            update_wrapper(wrapped_init, original_init)
            cls.__init__ = wrapped_init


            def attribute_profile_decorator(func: Callable, profiling_type: Optional[str], print2log: bool) -> Callable:
                """Decorator that adds log method calls and adds profiling to functions."""
                @wraps(func)
                def wrapper(*args: Any, **kwargs: Any) -> Any:
                    if verbose:
                        logger.debug(f"** {cls.__name__}.{func.__name__} ** Called", _log_system_msg="<LOG_MANAGER>")

                    try:
                        _result = manage_profiling(func, args, kwargs, profiling_type=profiling_type, print2log=print2log)
                    except Exception as e:
                        logger.error(f"Uncatched Error: {e}", _log_system_msg="<LOG_MANAGER>")
                        logger.error(trace_error_msg(), _log_system_msg="<LOG_MANAGER>")
                        if logger._debugging_msg:
                            logger.perror("------\nError Handling Suggestions: \n" + ";\n".join(logger._debugging_msg), _log_system_msg="<LOG_MANAGER>")
                        if profiling_type == "line":
                            try:
                                profiler.disable()
                                del profiler
                            except:
                                pass
                        raise

                    if verbose:
                            logger.debug(f"** {cls.__name__}.{func.__name__} ** Returned", _log_system_msg="<LOG_MANAGER>")
    
                    return _result
                return wrapper

            def profile_all_methods(cls: type, profiling_type: Optional[str], print2log: bool) -> None:
                """Class decorator that applies the `attr_profiler_decorator` decorator to all callable methods of a class."""
                for attr_name in dir(cls):
                    attr = getattr(cls, attr_name)
                    if callable(attr) and not attr_name.startswith("__"):
                        setattr(cls, attr_name, attribute_profile_decorator(attr, profiling_type, print2log))

            profile_all_methods(cls, enable_profiling, print2log)

            return cls

        @decorator.register(types.FunctionType)
        def _(func: Callable) -> Callable:
            @wraps(func)
            def wrapper(*args: Any, **kwargs: Any) -> Any:
                if verbose:
                    logger.debug(f'Function ** {func.__name__} ** Called', _log_system_msg="<LOG_MANAGER>")
                try:
                    kwargs.pop('log', None)
                    kwargs['log'] = logger
                    _result = manage_profiling(func, args, kwargs, profiling_type=enable_profiling, print2log=print2log)
                    if verbose:
                        logger.debug(f'Function ** {func.__name__} ** Returned.', _log_system_msg="<LOG_MANAGER>")
                    return _result
                except Exception as e:
                    logger.error(f"Uncatched Error: {e}", _log_system_msg="<LOG_MANAGER>")
                    logger.error(trace_error_msg(), _log_system_msg="<LOG_MANAGER>")
                    if logger._debugging_msg:
                            logger.perror("------\nError Handling Suggestions: \n" + ";\n".join(logger._debugging_msg), _log_system_msg="<LOG_MANAGER>")
                    raise
            return wrapper
        
        return decorator
    
    setup_check=staticmethod(setup_check)
