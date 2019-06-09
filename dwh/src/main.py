import ast
import json
from datetime import datetime
from typing import Dict
import os
import pandas as pd
from google.cloud import storage, bigquery
from sqlalchemy import create_engine


def main() -> None:
    def timestamp_to_datetime(ts: int) -> str:
        return datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

    def load_json(fp: str) -> Dict[str, str]:
        with open(fp, 'r') as f:
            data = json.load(f)
        return data

    config = {
        "host": 'localhost',
        "database": 'ted',
        "user": 'root',
        "password": '',
        "port": 3306
    }
    engine = create_engine('mysql://{user}:{password}@{host}:{port}/{database}'.format(**config))
    jobs = {
        "ted_main": "select * from ted.ted_main;",
        "transcripts": "select * from ted.transcripts;",
        "ted_all": """select t1.*, t2.transcript 
                   from ted.ted_main t1 
                   join ted.transcripts t2 on t1.url = t2.url 
                   where t2.url is not NULL;
                   """
    }
    gcs = storage.Client(project='bigquery-tutorial')
    bucket = gcs.bucket(bucket_name='ted_talks')
    bq = bigquery.Client(project='bigquery-tutorial-229808')
    dataset = bq.dataset(dataset_id='ted_talks')

    for table_name, sql in jobs.items():
        df = pd.read_sql(sql, engine)
        df.to_csv(f'{table_name}.csv', index=False)

        # Data Lake
        blob = bucket.blob(blob_name=f'raw/{table_name}.csv')
        blob.upload_from_filename(f'{table_name}.csv')
        os.remove(f'{table_name}.csv')

        # Transform
        if "ted" in table_name:
            df[['film_date', 'published_date']] = df[['film_date', 'published_date']].applymap(timestamp_to_datetime)
            df[['ratings', 'related_talks']] = df[['ratings', 'related_talks']].applymap(ast.literal_eval)

        # Save Transformed Data
        df.to_json(f'{table_name}.json', orient='records', lines=True)
        blob = bucket.blob(blob_name=f'processed/{table_name}.json')
        blob.upload_from_filename(f'{table_name}.json')
        os.remove(f'{table_name}.json')

        # load schemas and upload
        json_config = load_json(f"schemas/{table_name}.json")
        blob = bucket.blob(blob_name=f'schemas/{table_name}.json')
        if not blob.exists():
            blob.upload_from_filename(f'{table_name}.json')

        # Create SchemaFields
        config = []
        s: Dict
        for s in json_config:
            if 'fields' in s.keys():
                s['fields'] = tuple(bigquery.SchemaField(**c) for c in s['fields'])
            config.append(bigquery.SchemaField(**s))

        # Load to BQ
        config = bigquery.LoadJobConfig(
            source_format='NEWLINE_DELIMITED_JSON',
            write_disposition='WRITE_TRUNCATE',
            schema=config)
        bq.load_table_from_uri(
            blob.public_url,
            destination=dataset.table(f'{table_name}'),
            job_config=config)


if __name__ == '__main__':
    main()
