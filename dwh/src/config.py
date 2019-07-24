from dataclasses import dataclass
from typing import ClassVar


@dataclass(frozen=True)
class MySQL:
    host: ClassVar[str] = 'localhost'
    database: ClassVar[str] = 'ted'
    user: ClassVar[str] = 'root'
    password: ClassVar[str] = ''
    port: ClassVar[int] = 3306


@dataclass(frozen=True)
class GCP:
    bucket: ClassVar[str] = 'ted_talks'
    dataset_id: ClassVar[str] = 'ted_talks'
