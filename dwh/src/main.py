import ast

import pandas as pd
from google.cloud import bigquery

from dwh.src.data import rdb, gcs, bq
from dwh.src.path_manager import PathManger
from dwh.src.utils import timestamp_to_datetime, load_json


def main() -> None:
    mysql = rdb.mysql()
    gcs_client = gcs.client()
    bkt = gcs.bucket(gcs_client)

    bq_client = bq.client()
    bq_dataset = bq.dataset(bq_client)

    fps = PathManger.get_sql()
    for fp in fps:
        table_name = fp.stem

        #############
        # Extract
        #############
        with open(fp, 'r') as f:
            df = pd.read_sql(f.read(), mysql)

        #############
        # Data Lake
        #############
        df.to_csv(f"{table_name}.csv", index=False)
        gcs.upload_from_file(bkt, f"raw/{table_name}.csv", f"{table_name}.csv")

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
        gcs.upload_from_file(bkt, f"processed/{table_name}.json", f"{table_name}.json")

        #############
        # upload schemas
        #############
        json_config = load_json(f"schemas/{table_name}.json")
        gcs.upload_from_file(bkt, f"schemas/{table_name}.json", f"{table_name}.json", delete_file=False)

        # Create SchemaFields
        config = bq.schema_fields(json_config)
        bq.ingest_by_uri(cl=bq_client, destination=bq_dataset.table(table_name), uri=f'gs://{bkt.name}')

        # Load to BQ
        bq.ingest_by_uri(bq_client, bq_dataset.table(table_name), uri=)
        config = bigquery.LoadJobConfig(
            source_format="NEWLINE_DELIMITED_JSON",
            write_disposition="WRITE_TRUNCATE",
            schema=config,
        )
        bq.load_table_from_uri(
            blob.public_url,
            destination=dataset.table(f"{table_name}"),
            job_config=config,
        )


if __name__ == "__main__":
    main()
