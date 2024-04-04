from google.cloud import bigquery
import sys

def create_dataset_table(request):
  client = bigquery.Client()

  dataset_id = "stumpsndbails.dbtInput".format(client.project)
  dataset = bigquery.Dataset(dataset_id)
  dataset.location = "US"
  client.delete_dataset( dataset_id, delete_contents=True, not_found_ok=True )
  dataset = client.create_dataset(dataset, timeout=30)

  ball_table_id = "stumpsndbails.dbtInput.ball_by_ball"
  match_table_id = "stumpsndbails.dbtInput.match"
  player_table_id = "stumpsndbails.dbtInput.player"
  external_source_format = "CSV"
  ball_source_uris = [ "gs://stumpsndbails/staging/ballByball/*.csv"]
  match_source_uris = [ "gs://stumpsndbails/staging/match_info/*.csv"]
  player_source_uris = [ "gs://stumpsndbails/staging/player_info/*.csv"]


  external_config = bigquery.ExternalConfig(external_source_format)
  external_config.source_uris = ball_source_uris
  external_config.options.skip_leading_rows = 1
  ball_schema = [
    bigquery.SchemaField("match_id", "STRING"),
    bigquery.SchemaField("season", "STRING"),
    bigquery.SchemaField("start_date", "DATE"),
    bigquery.SchemaField("venue", "STRING"),
    bigquery.SchemaField("innings", "INT64"),
    bigquery.SchemaField("ball", "STRING"),
    bigquery.SchemaField("batting_team", "STRING"),
    bigquery.SchemaField("bowling_team", "STRING"),
    bigquery.SchemaField("striker", "STRING"),
    bigquery.SchemaField("non_striker", "STRING"),
    bigquery.SchemaField("bowler", "STRING"),
    bigquery.SchemaField("runs_off_bat", "INT64"),
    bigquery.SchemaField("extras", "INT64"),
    bigquery.SchemaField("wides", "INT64"),
    bigquery.SchemaField("noballs", "INT64"),
    bigquery.SchemaField("byes", "INT64"),
    bigquery.SchemaField("legbyes", "INT64"),
    bigquery.SchemaField("penalty", "STRING"),
    bigquery.SchemaField("wicket_type", "STRING"),
    bigquery.SchemaField("player_dismissed", "STRING"),
    bigquery.SchemaField("other_wicket_type", "STRING"),
    bigquery.SchemaField("other_player_dismissed", "STRING")
    ]

  table = bigquery.Table(ball_table_id, schema=ball_schema)  
  table.external_data_configuration = external_config
  client.delete_table(table, not_found_ok=True)
  table = client.create_table(table)  # Make an API request.
  print( f"Created table " + ball_table_id )

  external_config = bigquery.ExternalConfig(external_source_format)
  external_config.source_uris = match_source_uris
  external_config.options.skip_leading_rows = 1

  match_schema = [
    bigquery.SchemaField("match_id", "STRING"),
    bigquery.SchemaField("Teams", "STRING"),
    bigquery.SchemaField("gender", "STRING"),
    bigquery.SchemaField("date", "STRING"),
    bigquery.SchemaField("winner", "STRING"),
    bigquery.SchemaField("method", "STRING"),
    bigquery.SchemaField("outcome", "STRING"),
    bigquery.SchemaField("player_of_match", "STRING"),
    bigquery.SchemaField("umpire", "STRING"),
    bigquery.SchemaField("city", "STRING"),
    bigquery.SchemaField("venue", "STRING"),
    bigquery.SchemaField("event", "STRING")
    ]

  table = bigquery.Table(match_table_id, schema=match_schema)  
  table.external_data_configuration = external_config
  client.delete_table(table, not_found_ok=True)
  table = client.create_table(table)  # Make an API request.
  print( f"Created table " + match_table_id )

  external_config = bigquery.ExternalConfig(external_source_format)
  external_config.source_uris = player_source_uris
  external_config.options.skip_leading_rows = 1

  player_schema = [
    bigquery.SchemaField("Team", "STRING"),
    bigquery.SchemaField("player_name", "STRING"),
    bigquery.SchemaField("match_id", "STRING"),
    bigquery.SchemaField("registry", "STRING")
    ]

  table = bigquery.Table(player_table_id, schema=player_schema)  
  table.external_data_configuration = external_config
  client.delete_table(table, not_found_ok=True)
  table = client.create_table(table)  # Make an API request.
  print( f"Created table " + player_table_id )

  return '', 200