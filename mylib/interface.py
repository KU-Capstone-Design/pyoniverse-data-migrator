from abc import ABCMeta, abstractmethod
from typing import Iterable


class Driver(metaclass=ABCMeta):
    @abstractmethod
    def read(self, rel: str, n: int = None) -> Iterable[dict]:
        """
        limit를 지정하지 않으면 모든 문서를 가져온다.
        """
        pass

    @abstractmethod
    def write(self, rel: str, updated: Iterable[dict]) -> None:
        pass


class Migrator(metaclass=ABCMeta):
    @abstractmethod
    def migrate(self, rel: str) -> None:
        pass

    @abstractmethod
    def _convert(self, rel: str, src: Iterable[dict]) -> Iterable[dict]:
        pass
