from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
from pandas import DataFrame
from os import path
import glob
import pandas as pd
import csv
from google.cloud import storage
import os
import dask


if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@dask.delayed
def file_upload_to_gcp(file_path, bucket, object_key_prefix):
    object_key = f"{object_key_prefix}{file_path.split('/')[-1]}"
    blob = bucket.blob(object_key)
    blob.upload_from_filename(file_path)
    return "success"

@data_exporter
def export_data(data, *args, **kwargs):
    destination = "/home/src/rawData"
    csv_files = glob.glob(f"{destination}/*.csv")

    bucket_name = 'stumpsndbails'
    project_id = 'stumpsndbails'
    object_key_prefix = 'raw/'
    counter = 0

    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/src/secrets/service-account.json"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    delayed_results = []

    for csv_file in csv_files:
        delayed_results.append(file_upload_to_gcp(csv_file, bucket, object_key_prefix))

    dask.compute(*delayed_results)
    
    print("Raw Data Ingested successfully")
