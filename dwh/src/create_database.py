import pandas as pd
from sqlalchemy import create_engine


def main() -> None:
    config = {
        "host": 'localhost',
        "database": 'ted',
        "user": 'root',
        "password": '',
        "port": 3306
    }
    engine = create_engine('mysql://{user}:{password}@{host}:{port}/{database}'.format(**config))

    df = pd.read_csv('../../data/ted_main.csv')
    df.to_sql('ted_main', engine, if_exists='replace', index_label='id')

    df = pd.read_csv('../../data/transcripts.csv')
    df.to_sql('transcripts', engine, if_exists='replace', index_label='id')

if __name__ == '__main__':
    main()