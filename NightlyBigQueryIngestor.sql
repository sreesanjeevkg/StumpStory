CREATE OR REPLACE EXTERNAL TABLE `stumpsndbails.Crictest.ball_by_ball`
OPTIONS (
  format = 'CSV',
  uris = ['gs://stumpsndbails/staging/ballByball/*.csv']
);

CREATE OR REPLACE EXTERNAL TABLE `stumpsndbails.Crictest.match_info`
OPTIONS (
  format = 'CSV',
  uris = ['gs://stumpsndbails/staging/match_info/*.csv']
);

CREATE OR REPLACE EXTERNAL TABLE `stumpsndbails.Crictest.player_info`
OPTIONS (
  format = 'CSV',
  uris = ['gs://stumpsndbails/staging/player_info/*.csv']
);
