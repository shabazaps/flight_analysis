

CREATE EXTERNAL TABLE IF NOT EXISTS cancellation_reasons (
    CancellationCode STRING,
    Count BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs:///user/shabaz/flight_outputs/cancellation_reasons';