from typing import List, Dict, Any

from google.cloud import bigquery

from dwh.src.config import GCP


def client() -> bigquery.Client:
    return bigquery.Client()


def dataset(cl: bigquery.Client) -> bigquery.Dataset:
    return cl.dataset(dataset_id=GCP.dataset_id)


def schema_fields(config: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    # TODO: make more deep nest available
    c = []
    for s in config:
        if "fields" in s.keys():
            s["fields"] = tuple(bigquery.SchemaField(**c) for c in s["fields"])
        c.append(bigquery.SchemaField(**s))
    return c


def ingest_by_uri(cl: bigquery.Client, destination: str, uri: str,
                  schema: List[Any]) -> bigquery.LoadJob:
    c = bigquery.LoadJobConfig(
        source_format="NEWLINE_DELIMITED_JSON",
        write_disposition="WRITE_TRUNCATE",
        schema=schema
    )
    return cl.load_table_from_uri(uri, destination=destination, job_config=c)
