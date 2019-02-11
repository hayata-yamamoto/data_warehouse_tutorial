import os
from typing import Union

import pymysql.cursors


class MySQL:
    def __init__(self,
                 host: str,
                 user: str,
                 password: str,
                 db: str,
                 charset: str = 'utf8mb4',
                 autocommit: bool = True,
                 local_infile: bool = True,
                 cursor: pymysql.cursors.Cursor = pymysql.cursors.DictCursor):
        """

        Args:
            host (str) :
            user (str) :
            password (str) :
            db (str) :
            autocommit (bool) :
            local_infile (int or bool) :
        """
        if user == 'root':
            self.connection = pymysql.connect(
                host=host,
                user=user,
                db=db,
                autocommit=autocommit,
                local_infile=local_infile,
                charset=charset,
                cursorclass=cursor
            )
        else:
            self.connection = pymysql.connect(
                host=host,
                user=user,
                db=db,
                password=password,
                autocommit=autocommit,
                local_infile=local_infile,
                charset=charset,
                cursorclass=cursor
            )

    def load(self, fp: Union[str, os.PathLike], table: str) -> None:
        """

        Args:
            fp (str, PathLike):
            table (str) :

        Returns:
            str : sql query
        """
        sql = f"LOAD DATA INFILE '{str(fp)}' " \
              f"INTO TABLE {table} " \
              f"FIELDS TERMINATED BY ',' " \
              f"ENCLOSED BY '\"' " \
              f"LINES TERMINATED BY '\n' " \
              f"IGNORE 1 LINES;"
        with self.connection.cursor() as cursor:
            cursor.execute(sql)
