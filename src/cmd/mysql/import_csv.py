from src.model.mysql import MySQL


if __name__ == '__main__':
    mysql = MySQL(
        host='localhost',
        user='root',
        database='warehouse',
        autocommit=True,
        local_infile=1
    )





