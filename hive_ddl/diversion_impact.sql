CREATE EXTERNAL TABLE IF NOT EXISTS diversion_impact (
    Avg_Diverted_Elapsed_Time DOUBLE,
    Avg_Diverted_Arrival_Delay DOUBLE,
    Avg_Diverted_Distance DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs:///user/shabaz/flight_outputs/diversion_impact';