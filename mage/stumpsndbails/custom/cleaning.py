import os
import glob
import pandas as pd
import re
import numpy as np
import dask
if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

def extract_values(df, value):
    values = df[df.iloc[:, 0] == value].iloc[:, 1].values
    if len(values) == 0:
        values = None
    elif len(values) > 1:
        values = [", ".join(val.split("' '")) for val in values]
    return values

@dask.delayed
def cleaning(csv, max_cols):
    df = pd.read_csv(csv,header = None, names=range(max_cols), skiprows=1, usecols=[1,2,3,4])
    match = re.search(r"(\d+)_info\.csv", csv)
    match_id = match.group(1)
    df['match_id']=match_id

    players = df[df.iloc[:, 0] == 'player']
    players = players.iloc[:,[1,2,4]]
    players.columns = ['Team', 'player_name', 'match_id']

    people = df[df.iloc[:, 1] == 'people']
    people = people.iloc[:,[2,3,4]]
    people.columns = ['player_name', 'registry', 'match_id']


    player_info = pd.merge(players, people, how="inner", on=["player_name", "match_id"])

    player_info.to_csv(f"player_info_by_matches/player_info_{match_id}.csv", index=False)

    match_info_columns = ['match_id','Teams', 'gender', 'date', 'winner', 'method', 'outcome', 'player_of_match', 'umpire', 'city', 'venue','event']

    teams  = extract_values(df, 'team')
    gender = extract_values(df, 'gender')
    date = extract_values(df, 'date')
    winner = extract_values(df, 'winner')
    method = extract_values(df, 'method')
    outcome = extract_values(df, 'outcome')
    player_of_match = extract_values(df, 'player_of_match')
    umpire = extract_values(df, 'umpire')
    city = extract_values(df, 'city')
    venue = extract_values(df, 'venue')
    event = extract_values(df, 'event')

    match_info_df = pd.DataFrame(columns=match_info_columns)

    match_info_df['gender'] = gender
    match_info_df['Teams'] = [teams]
    match_info_df['match_id']=match_id # TODO: Why debug further
    match_info_df['date'] = [date]
    match_info_df['winner'] = winner
    match_info_df['method'] = method
    match_info_df['outcome'] = outcome
    match_info_df['player_of_match'] = [player_of_match]
    match_info_df['umpire'] = [umpire]
    match_info_df['city'] = city
    match_info_df['venue'] = venue
    match_info_df['event'] = event

    match_info_df.to_csv(f"match_info_by_matches/match_info_{match_id}.csv", columns=match_info_columns, index=False)

    return "success"




@custom
def transform_custom(*args, **kwargs):
    """
    args: The output from any upstream parent blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your custom logic here

    directory_path = "/home/src/rawData"
    player_info_path = "/home/src/player_info_by_matches"
    match_info_path = "/home/src/match_info_by_matches"
    csv_files_info = glob.glob(f"{directory_path}/*_info.csv")

    if not os.path.exists(player_info_path):
        os.mkdir(player_info_path)
    
    if not os.path.exists(match_info_path):
        os.mkdir(match_info_path)

    max_cols = 5
    delayed_results = []

    for csv in csv_files_info:
        delayed_results.append(cleaning(csv, max_cols))

    dask.compute(*delayed_results)

    return None
