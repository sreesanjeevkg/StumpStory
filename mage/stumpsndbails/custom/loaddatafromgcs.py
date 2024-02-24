from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
from pandas import DataFrame
import os
import glob
import pandas as pd
import csv
from google.cloud import storage
import shutil
from pathlib import Path
if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test



@custom
def transform_custom(*args, **kwargs):
    """
    args: The output from any upstream parent blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your custom logic here

    dl_dir = "/home/src/rawData"

    bucket_name = 'stumpsndbails_storage_bucket'
    project_id = 'stumpsndbails'
    object_key_prefix = 'raw/'
    counter = 0

    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/src/secrets/service-account.json"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=object_key_prefix)

    if not os.path.exists(dl_dir):
        os.makedirs(dl_dir)

    for blob in blobs:
        if blob.name.endswith('/'):
            continue
        counter += 1
        if (counter % 1000 == 0):
            print(counter, "files downloaded")
        filename = blob.name.split('/', 1)[-1]
        blob.download_to_filename(dl_dir+"/"+filename)

    return None
