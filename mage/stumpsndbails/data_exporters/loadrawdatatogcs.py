from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
from pandas import DataFrame
from os import path
import glob
import pandas as pd

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_google_cloud_storage(df: DataFrame, **kwargs) -> None:
    """
    Template for exporting data to a Google Cloud Storage bucket.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#googlecloudstorage
    """
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'
    destination = "/home/src/rawData"
    csv_files = glob.glob(f"{destination}/*[0-9].csv")

    bucket_name = 'stumpsndbails_storage_bucket'
    object_key_prefix = 'raw/'

    for csv_file in csv_files:
        df = pd.read_csv(csv_file)
        object_key = f"{object_key_prefix}{csv_file.split('/')[-1]}"
        GoogleCloudStorage.with_config(ConfigFileLoader(config_path, config_profile)).export(
        df,
        bucket_name,
        object_key
    )
