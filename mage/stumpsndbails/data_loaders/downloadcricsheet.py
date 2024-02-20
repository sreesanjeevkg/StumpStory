import io
import pandas as pd
import requests
import zipfile
import glob
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """
    url = 'https://cricsheet.org/downloads/all_csv2.zip'
    response = requests.get(url)
    destination = "/home/src/rawData"

    if response.status_code == 200:
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            z.extractall(destination)
        print("Extracted successfully")
        
    return None

@test
def test_output(*args) -> None:
    """
    Template code for testing the output of the block.
    """
    path = "/home/src/rawData"
    info_pattern = f"{path}/[0-9]*_info.csv"
    data_pattern = f"{path}/[0-9]*.csv"

    info_files = glob.glob(info_pattern)
    data_files = glob.glob(data_pattern)

    result = bool(info_files) and bool(data_files)
    
    assert result is True, 'No files matching the pattern was found'

