from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, List


@dataclass(kw_only=True, frozen=True)
class Data:
    column: str = field(default=None)
    value: Any = field(default=None)


@dataclass(kw_only=True, frozen=True)
class Filter:
    column: str = field(default=None)
    op: str = field(default=None)
    value: Any = field(default=None)


@dataclass(kw_only=True)
class Message:
    origin: str = field(default=None)
    date: str = field(default=None)
    db_name: str = field(default=None)
    rel_name: str = field(default=None)
    action: str = field(default=None)
    filters: List[Filter] = field(default_factory=list)
    data: List[Data] = field(default_factory=list)

    def __post_init__(self):
        # %Y-%m-%dT%H:%M:%S Format
        self.date = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
