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
    directory_path = "/home/src/rawData"
    player_info_path = "/home/src/player_info_by_matches"
    match_info_path = "/home/src/match_info_by_matches"
    csv_files = glob.glob(f"{directory_path}/*[0-9].csv")
    player_info_csv_files = glob.glob(f"{player_info_path}/*.csv")
    match_info_csv_files = glob.glob(f"{match_info_path}/*.csv")

    bucket_name = 'stumpsndbails'
    project_id = 'stumpsndbails'
    csv_object_key_prefix = 'staging/ballByball/'
    player_info_prefix = 'staging/player_info/'
    match_info_prefix = 'staging/match_info/'
    counter = 0

    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/src/secrets/service-account.json"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    delayed_results = []

    for csv_file in csv_files:
        delayed_results.append(file_upload_to_gcp(csv_file, bucket, csv_object_key_prefix))
    
    for csv_file in player_info_csv_files:
        delayed_results.append(file_upload_to_gcp(csv_file, bucket, player_info_prefix))
    
    for csv_file in match_info_csv_files:
        delayed_results.append(file_upload_to_gcp(csv_file, bucket, match_info_prefix))

    
    dask.compute(*delayed_results)

    print("Staging Data Ingested successfully")
