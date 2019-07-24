import ast
import logging

import pandas as pd

from dwh.src.data import rdb, gcs, bq
from dwh.src.path_manager import PathManger
from dwh.src.utils import timestamp_to_datetime, load_json


def main() -> None:
    mysql = rdb.mysql()
    gcs_client = gcs.client()
    bkt = gcs.bucket(gcs_client)

    bq_client = bq.client()
    bq_dataset = bq.dataset(bq_client)
    logging.basicConfig(level=logging.INFO)

    fps = PathManger.get_sql()
    for fp in fps:
        table_name = fp.stem

        #############
        # Extract
        #############
        with open(fp, 'r') as f:
            df = pd.read_sql(f.read(), mysql)
        logging.info('extract from mysql')

        #############
        # Data Lake
        #############
        df.to_csv(f"{table_name}.csv", index=False)
        gcs.upload_from_file(bkt, f"raw/{table_name}.csv", f"{table_name}.csv")
        logging.info('upload csv')

        #############
        # Transform
        #############
        if "ted" in table_name:
            df[["film_date", "published_date"]] = df[
                ["film_date", "published_date"]
            ].applymap(timestamp_to_datetime)
            df[["ratings", "related_talks"]] = df[
                ["ratings", "related_talks"]
            ].applymap(ast.literal_eval)

        #############
        # Save Transformed Data
        #############
        df.to_json(f"{table_name}.json", orient="records", lines=True)
        uri = gcs.upload_from_file(bkt, f"processed/{table_name}.json", f"{table_name}.json")
        logging.info('upload transformed json')

        #############
        # upload schemas
        #############
        j = str(PathManger.SCHEMAS / f"{table_name}.json")
        json_config = load_json(j)
        gcs.upload_from_file(bkt, f'schemas/{table_name}.json', j, delete_file=False)
        logging.info('upload schema file')

        #############
        # BQ load
        #############
        schemas = bq.schema_fields(json_config)
        bq.ingest_by_uri(
            cl=bq_client,
            destination=bq_dataset.table(table_name),
            uri=uri,
            schema=schemas)
        logging.info('start ingest job')


if __name__ == "__main__":
    main()
