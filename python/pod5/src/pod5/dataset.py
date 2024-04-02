from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache, partial
import os
from pathlib import Path
from typing import (
    Callable,
    Collection,
    Dict,
    Generator,
    Iterable,
    List,
    Optional,
    Set,
    Union,
)
import warnings
from pod5.api_utils import Pod5ApiException

from pod5.pod5_types import PathOrStr
from pod5.reader import ReadRecord, Reader
from pod5.tools.utils import search_path

DEFAULT_CPUS = os.cpu_count() or 1


class DatasetReader:
    def __init__(
        self,
        paths: Union[PathOrStr, Collection[PathOrStr]],
        recursive: bool = False,
        pattern: str = "*.pod5",
        index: bool = False,
        threads: int = DEFAULT_CPUS,
        max_cached_readers: Optional[int] = 2**4,
        warn_duplicate_indexing: bool = True,
    ) -> None:
        """
        Reads pod5 files and/or directories of pod5 files as a dataset.

        Parameters
        ----------
        paths : PathOrStr | Collection[PathOrStr]
            One or more files or directories to load
        recursive : bool
            Search directories in `paths` recursively
        pattern : str
            A glob expression to match against file names
        index : bool
            Promptly index the dataset instead of deferring until required
        threads : int
            The number of threads to use
        max_cached_readers :  Optional[int]
            The maximum size of the `Reader` LRU cache. Set to `None` for an unlimited
            cache size.
        warn_duplicate_indexing : bool
            Issue warnings when duplicate read_ids are detected and
            indexing by read_id is attempted

        Note
        ----
        Random record access is implemented by creating an index of read_id to file
        path. This can consume a large amount of memory. Methods that generate an index
        have this noted in their docstring.

        Warnings
        --------
        If duplicate read_ids are present in the dataset, iterator methods such
        as `reads()` will yield all copies. Indexing methods such as `get_read`
        return one chosen randomly and issue a warning which can be suppressed by
        setting `warn_duplicate_indexing=False`
        """
        self._paths: List[Path] = sorted(
            self._collect_dataset(
                paths, recursive=recursive, pattern=pattern, threads=threads
            )
        )
        self._num_reads: Optional[int] = None
        self._max_cached_readers = max_cached_readers
        self.threads = threads
        self.warn_duplicate_indexing = warn_duplicate_indexing

        # Cache on DatasetReader instances and control cache size on init
        self._get_reader = self._init_get_reader(self._max_cached_readers)

        if index:
            self._index_read_ids()
        else:
            self._index: Optional[Dict[str, Path]] = None

    def __iter__(self) -> Generator[ReadRecord, None, None]:
        yield from self.reads()

    def __len__(self) -> int:
        """Returns the number of reads in this dataset"""
        return self.num_reads

    @property
    def num_reads(self) -> int:
        """
        Return the number of `ReadRecords` in this dataset.
        """
        if self._num_reads is not None:
            return self._num_reads

        def _get_num_reads(path: Path) -> int:
            try:
                return self.get_reader(path).num_reads
            except Exception as exc:
                msg = f"DatasetReader error reading: {[path]}"
                raise Pod5ApiException(msg) from exc

        self._num_reads = 0
        with ThreadPoolExecutor(max_workers=self.threads) as executor:
            self._num_reads = sum(executor.map(_get_num_reads, self._paths))

        return self._num_reads

    @property
    def paths(self) -> List[Path]:
        """Return the list of pod5 file paths in this dataset"""
        return self._paths

    @property
    def read_ids(self) -> Generator[str, None, None]:
        """
        Yield all read_ids in this dataset
        """

        def _get_read_ids(path: Path):
            return self.get_reader(path).read_ids

        with ThreadPoolExecutor(max_workers=self.threads) as executor:
            for read_id_gen in executor.map(_get_read_ids, self.paths):
                yield from read_id_gen

    def reads(
        self,
        selection: Optional[Iterable[str]] = None,
        preload: Optional[Set[str]] = None,
    ) -> Generator[ReadRecord, None, None]:
        """
        Iterate over ``ReadRecord``s in the dataset.

        Parameters
        ----------
        selection : iterable[str]
            The read ids to walk in the file.
        preload : set[str]
            Columns to preload - "samples" and "sample_count" are valid values

        Note
        ----
        ``ReadRecord``s are yielded in on-disk record order for each file in ``self.paths``.

        Missing records are not detected and multiple records will be
        yielded if there are duplicates in either of the dataset or selection.

        Yields
        ------
        :py:class:`ReadRecord`
        """

        def _get_reads_iter(path: Path) -> Generator[ReadRecord, None, None]:
            return self.get_reader(path).reads(
                selection=selection, missing_ok=True, preload=preload
            )

        with ThreadPoolExecutor(max_workers=self.threads) as executor:
            for read_gen in executor.map(_get_reads_iter, self.paths):
                yield from read_gen

    def get_read(self, read_id: str) -> Optional[ReadRecord]:
        """
        Get a `ReadRecord` by `read_id` or return `None` if it is missing

        Parameters
        ----------
        read_id : str
            The read_id (UUID) string in this dataset to find

        Note
        ----
        This method will index the dataset

        Warnings
        --------
        Issues a warning if duplicate read_ids are detected in this dataset.
        The returned `ReadRecord` is a always valid but the source may be random
        between instances of a `DatasetReader`.

        Returns
        -------
        A :py:class:`ReadRecord` or `None`
        """
        path = self.get_path(read_id)
        if path is None:
            return None

        reader = self.get_reader(path)
        try:
            return next(reader.reads(selection=[read_id]))
        except StopIteration:
            return None

    @staticmethod
    def _init_get_reader(maxsize: Optional[int]) -> Callable[[Path], Reader]:
        # This wrapper allows the size of the LRU cache to be set during initialization
        # without global variables
        @lru_cache(maxsize=maxsize)
        def _get_reader(path: Path) -> Reader:
            return Reader(path)

        return _get_reader

    def get_reader(self, path: PathOrStr) -> Reader:
        """
        Get a pod5 file `Reader` in this dataset by `path`

        Parameters
        ----------
        path : PathOrStr
            Path to a pod5 file

        Returns
        -------
        A :py:class:`Reader`
        """
        return self._get_reader(Path(path))

    def get_path(self, read_id: str) -> Optional[Path]:
        """
        Get the pod5 `Path` for a given `read_id` or `None` if it was not found

        Parameters
        ----------
        read_id : str
            The read_id (UUID) string in this dataset

        Note
        ----
        This method will index the dataset

        Warnings
        --------
        Issues a warning if duplicate read_ids are detected in this dataset.
        The returned path is a always valid file which contains this read_id but this
        may be random between instances.

        Returns
        -------
        A `Path` or `None`
        """
        if self._index is None:
            self._index_read_ids()

        if self._index is None:
            return None

        if self.has_duplicate():
            self._issue_duplicate_read_warning()

        return self._index.get(read_id, None)

    def clear_readers(self) -> None:
        """Clears the readers LRU cache"""
        self._get_reader.cache_clear()  # type: ignore

    def clear_index(self) -> None:
        """Clears the read_id to file path index"""
        self._index = None

    def has_duplicate(self) -> bool:
        """
        Returns `True` if there are duplicate `read_ids` in this dataset

        Note
        ----
        This method will index the dataset
        """
        if self._index is None:
            self._index_read_ids()
        assert self._index is not None
        return len(self) != len(self._index)

    @staticmethod
    def _collect_dataset(
        paths: Union[PathOrStr, Collection[PathOrStr]],
        recursive: bool,
        pattern: str,
        threads: int,
    ) -> Set[Path]:
        if isinstance(paths, (str, Path, os.PathLike)):
            paths = [paths]

        if not isinstance(paths, Collection):
            raise TypeError(
                f"paths must be a Collection[PathOrStr] but found {type(paths)=}"
            )

        paths = [Path(p) for p in paths]
        collected: Set[Path] = set()
        with ThreadPoolExecutor(max_workers=threads) as executor:
            search = partial(search_path, recursive=recursive, patterns=[pattern])
            for coll in executor.map(search, paths):
                collected.update(coll)
        return collected

    def _index_read_ids(self) -> None:
        self._index = {}

        def _get_index(path: Path) -> Dict[str, Path]:
            try:
                return {read_id: path for read_id in self.get_reader(path).read_ids}
            except Exception as exc:
                msg = f"DatasetReader error reading: {[path]}"
                raise Pod5ApiException(msg) from exc

        with ThreadPoolExecutor(max_workers=self.threads) as executor:
            for index in executor.map(_get_index, self.paths):
                self._index.update(index)

    def _issue_duplicate_read_warning(self) -> None:
        if self.warn_duplicate_indexing:
            warnings.warn("duplicate read_ids found in dataset")

    def __enter__(self) -> "DatasetReader":
        return self

    def __exit__(self, *exc_details) -> None:
        self.clear_index()
        self.clear_readers()
