CREATE EXTERNAL TABLE IF NOT EXISTS top10_arrival_delay_airlines (
    IATA_CODE_Reporting_Airline STRING,
    count BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs:///user/shabaz/flight_outputs/top10_arrival_delay_airlines';