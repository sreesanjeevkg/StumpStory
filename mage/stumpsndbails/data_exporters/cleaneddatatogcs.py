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

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


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

    for csv_file in csv_files:
        counter += 1
        if (counter % 1000 == 0):
            print(counter, "files uploaded") 
        object_key = f"{csv_object_key_prefix}{csv_file.split('/')[-1]}"
        blob = bucket.blob(object_key)
        blob.upload_from_filename(csv_file)

    print("Finished Ball by Ball")
    
    for csv_file in player_info_csv_files:
        counter += 1
        if (counter % 1000 == 0):
            print(counter, "files uploaded") 
        object_key = f"{player_info_prefix}{csv_file.split('/')[-1]}"
        blob = bucket.blob(object_key)
        blob.upload_from_filename(csv_file)

    print("Finished Player Infos by match")
    
    for csv_file in match_info_csv_files:
        counter += 1
        if (counter % 1000 == 0):
            print(counter, "files uploaded") 
        object_key = f"{match_info_prefix}{csv_file.split('/')[-1]}"
        blob = bucket.blob(object_key)
        blob.upload_from_filename(csv_file)
    
    print("Finished Match Infos by match")

    print("Staging Data Ingested successfully")
