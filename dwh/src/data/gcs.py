from google.cloud import storage
import os

from dwh.src.config import GCP


def client() -> storage.Client:
    return storage.Client(project=GCP.project_id)


def bucket(cl: storage.Client) -> storage.Bucket:
    return cl.bucket(bucket_name=GCP.bucket)


def upload_from_file(bkt: storage.Bucket, blob: str, file: str, delete_file: bool = True) -> None:
    b = bkt.blob(blob_name=blob)
    b.upload_from_filename(file)
    if delete_file:
        os.remove(file)
