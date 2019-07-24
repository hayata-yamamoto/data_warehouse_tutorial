import pandas as pd
from sqlalchemy import create_engine, engine

from dwh.src.config import MySQL


def mysql() -> engine.Engine:
    return create_engine(
        f"mysql://{MySQL.host}:{MySQL.password}@{MySQL.host}:{MySQL.port}/{MySQL.database}"
    )


def ingest(
    engine: engine.Engine,
    df: pd.DataFrame,
    table: str,
    if_exsits: str = "replace",
    index_label: str = "id",
) -> None:
    df.to_sql(table, engine, if_exists=if_exsits, index_label=index_label)
