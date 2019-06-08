import mysql.connector


def main() -> None:
    conn = mysql.connector.connect(host='localhost', user='root', database='ted')
    cursor = conn.cursor()

    cursor.execute('select * from ted_main limit 10;')
    for r in cursor:
        print(r)



if __name__ == '__main__':
    main()