CREATE EXTERNAL TABLE IF NOT EXISTS diversion_rate_by_airline (
    Reporting_Airline STRING,
    Total_Flights BIGINT,
    Diverted_Flights BIGINT,
    Diversion_Rate DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs:///user/shabaz/flight_outputs/diversion_rate_by_airline';