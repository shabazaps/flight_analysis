CREATE EXTERNAL TABLE IF NOT EXISTS cancellation_rate_by_month (
    Month INT,
    Total_Flights BIGINT,
    Cancelled_Flights BIGINT,
    Cancellation_Rate DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs:///user/shabaz/flight_outputs/cancellation_rate_by_month';