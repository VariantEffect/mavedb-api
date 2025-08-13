import abc
from typing import Any

class BaseReader(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def fetch(self, seq_id: Any, start: Any, end: Any) -> Any: ...
    def __getitem__(self, ac: Any) -> Any: ...

class BaseWriter(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def store(self, seq_id: Any, seq: Any) -> Any: ...
