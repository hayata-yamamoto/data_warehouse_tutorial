from pathlib import Path
from typing import List


class PathManger:
    ROOT: Path = Path(__file__).resolve().parents[2]
    DATA: Path = ROOT / "data"
    BASE: Path = ROOT / "dwh"
    SQL: Path = BASE / "sql"
    SCHEMAS: Path = BASE / "schemas"
    SRC: Path = BASE / "src"
    TASKS: Path = BASE / "tasks"

    @staticmethod
    def get_sql() -> List[Path]:
        return [p for p in PathManger.SQL.iterdir()]

    @staticmethod
    def get_schemas() -> List[Path]:
        return [s for s in PathManger.SCHEMAS.iterdir()]
