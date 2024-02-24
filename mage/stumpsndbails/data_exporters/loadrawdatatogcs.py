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
import shutil

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data(data, *args, **kwargs):
    destination = "/home/src/rawData"
    csv_files = glob.glob(f"{destination}/*.csv")

    bucket_name = 'stumpsndbails_storage_bucket'
    project_id = 'stumpsndbails'
    object_key_prefix = 'raw/test/'
    counter = 0

    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/src/secrets/service-account.json"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    csv_files = ['/home/src/rawData/64012.csv']

    for csv_file in csv_files:
        counter += 1
        if (counter % 1000 == 0):
            print(counter, "files uploaded") 
        object_key = f"{object_key_prefix}{csv_file.split('/')[-1]}"
        blob = bucket.blob(object_key)
        blob.upload_from_filename(csv_file)
    
    print("Raw Data Ingested successfully")
