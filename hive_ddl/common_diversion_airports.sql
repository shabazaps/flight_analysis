
CREATE EXTERNAL TABLE IF NOT EXISTS common_diversion_airports (
    Div1Airport STRING,
    Diversions BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs:///user/shabaz/flight_outputs/common_diversion_airports';