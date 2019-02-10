from pathlib import Path
import pymysql.cursors

from src.model.mysql.load import load

if __name__ == '__main__':
    con = pymysql.connect(
        host='localhost',
        user='root',
        database='warehouse',
        autocommit=True,
        local_infile=1
    )
    p = Path(__file__).resolve().parents[3].joinpath('data', 'raw')
    with con.cursor() as cursor:
        for files in p.iterdir():



