

CREATE EXTERNAL TABLE IF NOT EXISTS top10_departure_delay_airports (
    ORIGIN STRING,
    count BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs:///user/shabaz/flight_outputs/top10_departure_delay_airports';
