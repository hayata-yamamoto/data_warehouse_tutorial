from pathlib import Path
from src.models.mysql import MySQL
import pymysql.cursors

if __name__ == '__main__':
    mysql = MySQL(
        host='localhost',
        user='root',
        db='warehouse',
        password='',
        autocommit=True,
        local_infile=True,
    )
    con = pymysql.connect(
        host='localhost',
        user='root',
        db='warehouse',
        password='',
        autocommit=True,
        local_infile=True,
    )

    files = Path(__file__).resolve().parents[3].joinpath('data', 'raw').glob('application_train.csv')

    for file in files:
        mysql.load(file, file.stem)
    # with con.cursor() as cursor:
    #     for file in files:

            # print(file, file.stem)
            # sql = f"LOAD DATA INFILE '{str(file)}' " \
            #       f"INTO TABLE {file.stem} " \
            #       f"FIELDS TERMINATED BY ',' ENCLOSED BY '\"' " \
            #       f"IGNORE 1 LINES;"
            # cursor.execute(sql)
