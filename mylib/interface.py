from abc import ABCMeta, abstractmethod
from typing import Iterable


class Driver(metaclass=ABCMeta):
    @abstractmethod
    def read(self, db: str, rel: str, n: int) -> Iterable[dict]:
        pass

    @abstractmethod
    def write(self, db: str, rel: str, updated: Iterable[dict]) -> None:
        pass


class Migrator(metaclass=ABCMeta):
    @abstractmethod
    def migrate(self) -> None:
        pass