CREATE EXTERNAL TABLE IF NOT EXISTS national_avg_delay (
    Avg_Arrival_Delay DOUBLE,
    Avg_Departure_Delay DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs:///user/shabaz/flight_outputs/national_avg_delay';