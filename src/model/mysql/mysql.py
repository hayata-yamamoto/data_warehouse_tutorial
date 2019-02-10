import os
from typing import Union

import pymysql.cursors


class MySQL:
    def __init__(
            self,
            host: str,
            user: str,
            password: str,
            database: str,
            autocommit: bool = True,
            local_infile: Union[int, bool] = 1):
        """

        Args:
            host (str) :
            user (str) :
            password (str) :
            database (str) :
            autocommit (bool) :
            local_infile (int or bool) :
        """
        self.connection = pymysql.connect(
            host=host,
            user=user,
            database=database,
            password=password,
            autocommit=autocommit,
            local_infile=local_infile
        )

    def load(self, fp: Union[str, os.PathLike], table: str) -> None:
        """

        Args:
            fp (str, PathLike):
            table (str) :

        Returns:
            str : sql query
        """
        sql = f"""
        LOAD DATA INFILE {str(fp)}
        INTO TABLE {table}
        COLUMNS TERMINATED BY ','
        OPTIONALLY ENCLOSED BY '"'
        ESCAPED BY '"'
        LINES TERMINATED BY '\n'
        IGNORE 1 LINES;
        """
        with self.connection.cursor() as cursor:
            cursor.execute(self.connection, sql)
