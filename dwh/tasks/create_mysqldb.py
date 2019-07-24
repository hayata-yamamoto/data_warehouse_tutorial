import pandas as pd
from dwh.src.data.rdb import mysql_engine, ingest


def main() -> None:
    mysql = mysql_engine()

    df = pd.read_csv("../../data/ted_main.csv")
    ingest(engine=mysql, df=df, table='ted_main')

    df = pd.read_csv("../../data/transcripts.csv")
    ingest(engine=mysql, df=df, table='transcripts')


if __name__ == "__main__":
    main()
