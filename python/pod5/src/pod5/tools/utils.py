"""
Utility functions for pod5 tools
"""

import datetime
import functools
import glob
import logging
import multiprocessing as mp
from multiprocessing.context import SpawnProcess
import os
from time import perf_counter
from typing import Collection, Iterable, List, Set, Union
from pathlib import Path
import uuid


# os.cpu_count() can return None if it fails
DEFAULT_THREADS = min(os.cpu_count() or 4, 4)


def init_logging():
    """Initialise logging only if POD5_DEBUG is true"""
    if not is_pod5_debug():
        logger = logging.getLogger("pod5")
        logger.addHandler(logging.NullHandler())
        return logger

    datetime_now = datetime.datetime.now().strftime("%Y-%m-%d--%H-%M-%S")
    if mp.current_process().name == "MainProcess":
        pid = "main"
    else:
        pid = f"p-{os.getpid()}"

    logger = logging.getLogger("pod5")
    logger.setLevel(logging.DEBUG)
    file_handler = logging.FileHandler(filename=f"{datetime_now}-{pid}-pod5.log")
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    )
    file_handler.setLevel(logging.DEBUG)
    logger.addHandler(file_handler)
    return logger


def logged(log_return: bool = False, log_args: bool = False, log_time: bool = False):
    """Logging parameterised decorator"""

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            logger = logging.getLogger("pod5")
            uid = f"{str(uuid.uuid4())[:2]}:'{func.__name__}'"
            if log_args:
                logger.debug("{0}:{1}, {2}".format(uid, args, kwargs))
            else:
                logger.debug("{0}".format(uid))
            try:
                started = perf_counter()
                ret = func(*args, **kwargs)
            except Exception as exc:
                logger.debug("{0}:Exception:{1}".format(uid, exc))
                raise exc
            if log_time:
                duration_s = perf_counter() - started
                logger.debug("{0}:Done:{1:.3f}s".format(uid, duration_s))
            if log_return:
                logger.debug("{0}:Returned:{1}".format(uid, ret))
            return ret

        return wrapper

    return decorator


logged_all = logged(log_return=True, log_args=True, log_time=True)


@logged_all
def terminate_processes(processes: List[SpawnProcess]) -> None:
    """terminate all child processes"""
    for proc in processes:
        try:
            proc.terminate()
        except ValueError:
            # Catch ValueError raised if proc is already closed
            pass
    return


@logged(log_return=True)
def limit_threads(requested: int) -> int:
    """
    Santise and limit the number of ``requested`` threads to the number of logical cores
    """
    if requested < 1:
        return os.cpu_count() or 4
    return min(os.cpu_count() or requested, requested)


@logged(log_time=True)
def collect_inputs(
    paths: Iterable[Path],
    recursive: bool,
    pattern: Union[str, Collection[str]],
    threads: int = DEFAULT_THREADS,
) -> Set[Path]:
    """
    Returns a set of `path` which match any of the given glob-style `pattern`s

    If a path is a directory this will be globbed (optionally recursively).
    If a path is a file then it must also match any of the given `pattern`s.

    Raises FileExistsError if any inputs do not exist
    """
    paths = set(paths)
    assert_inputs_exist(paths)
    if len(paths) == 0:
        raise AssertionError("Got 0 input paths to search")

    return search_paths(paths, recursive, pattern, min(threads, len(paths)))


@logged(log_time=True, log_return=True)
def assert_inputs_exist(inputs: Iterable[Path]):
    """Assert all inputs exist. Raises FileExistsError otherwise"""
    bad_paths = set()
    for path in set(inputs):
        if not path.exists():
            bad_paths.add(path)

    if bad_paths:
        raise FileExistsError(f"{len(bad_paths)} inputs do not exist: {bad_paths}")


@logged(log_time=True)
def search_paths(
    paths: Iterable[Path],
    recursive: bool,
    pattern: Union[str, Collection[str]],
    threads: int = DEFAULT_THREADS,
) -> Set[Path]:
    """
    Search all `paths` matching any of `patterns` searching directories recursively
    if requested
    """
    if isinstance(pattern, str):
        pattern = [pattern]

    srch = functools.partial(search_path, recursive=recursive, patterns=pattern)

    all_matches: Set[Path] = set()
    with mp.Pool(processes=threads) as pool:
        for matches in pool.imap_unordered(srch, paths):
            all_matches.update(matches)

    return all_matches


@logged(log_time=True)
def search_path(path: Path, recursive: bool, patterns: Collection[str]) -> Set[Path]:
    """
    Search `path` matching `pattern` searching directories recursively if requested
    """

    def _any_match(path: Path):
        return any(path.match(p) for p in patterns)

    # Get the recursive or non-recursive glob function.
    matching_files = set()
    if path.is_dir():
        pattern = str(path / "**" / "*") if recursive else str(path / "*")
        for matching_pathname in glob.glob(pattern, recursive=recursive):
            matching_path = Path(matching_pathname)
            if matching_path.is_file() and _any_match(matching_path):
                matching_files.add(matching_path)

    # Non-directory, assert that it is a file and that it matches the file_pattern
    elif path.is_file() and _any_match(path):
        matching_files.add(path)

    return matching_files


@logged(log_time=True)
def assert_no_duplicate_filenames(inputs: Collection[Path]) -> None:
    """
    Raises ValueError if there are duplicate filenames in the collection of Paths
    """
    names = [path.name for path in inputs]
    if len(names) != len(set(names)):
        raise ValueError(
            "One or more inputs share the same filename. "
            "This would cause a files to be overwritten at runtime"
        )


# Do not log this function as it's executed at import time
def is_disable_pbar() -> bool:
    """Check if POD5_PBAR is set returning true if PBAR should be disabled"""
    try:
        enabled = bool(int(os.environ.get("POD5_PBAR", "1")))
        return not enabled
    except Exception:
        return False


PBAR_DEFAULTS = dict(
    disable=is_disable_pbar(),
    smoothing=0.0,
    dynamic_ncols=True,
    ascii=True,
)


# Do not log this function as it's executed during logging initialisation
def is_pod5_debug() -> bool:
    """Check if POD5_DEBUG is set"""
    try:
        debug = bool(int(os.environ.get("POD5_DEBUG", "0")))
        return debug
    except Exception:
        return True
