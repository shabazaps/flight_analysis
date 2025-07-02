from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, when

hdfs_base_path = "hdfs:///user/shabaz/flight_outputs"

spark = SparkSession.builder \
    .appName("FlightDataTransform") \
    .enableHiveSupport() \
    .getOrCreate()

df = spark.read.parquet("s3://shabaz_bucket/us_flights_data/raw/")
df.createOrReplaceTempView("flights")


top10_depart = (df.filter(col("DEP_DELAY") > 15).groupBy("ORIGIN").agg(count("*").alias("count")).orderBy(col("count").desc()).limit(10)
)

top10_depart.write.mode("overwrite").option("header", True).csv(f"{hdfs_base_path}/top10_departure_delay_airports")


top10_arrival = (df.filter(col("ARR_DELAY") > 15).groupBy("OP_UNIQUE_CARRIER").agg(count("*").alias("count")).orderBy(col("count").desc()).limit(10).withColumnRenamed("OP_UNIQUE_CARRIER", "IATA_CODE_Reporting_Airline")
)

top10_arrival.write.mode("overwrite").option("header", True).csv(f"{hdfs_base_path}/top10_arrival_delay_airlines")


national_avg = df.select(avg("ARR_DELAY").alias("Avg_Arrival_Delay"),avg("DEP_DELAY").alias("Avg_Departure_Delay"))

national_avg.write.mode("overwrite").option("header", True).csv(f"{hdfs_base_path}/national_avg_delay")


cxl_by_airline = (df.groupBy("OP_UNIQUE_CARRIER").agg(count("*").alias("Total_Flights"),count(when(col("CANCELLED") == 1, True)).alias("Cancelled_Flights")).withColumn("Cancellation_Rate", col("Cancelled_Flights") / col("Total_Flights")).withColumnRenamed("OP_UNIQUE_CARRIER", "Reporting_Airline"))

cxl_by_airline.write.mode("overwrite").option("header", True).csv(f"{hdfs_base_path}/cancellation_rate_by_airline")


cxl_by_airport = (df.groupBy("ORIGIN").agg(count("*").alias("Total_Flights"), count(when(col("CANCELLED") == 1, True)).alias("Cancelled_Flights")).withColumn("Cancellation_Rate", col("Cancelled_Flights") / col("Total_Flights"))      .withColumnRenamed("ORIGIN", "Origin"))

cxl_by_airport.write.mode("overwrite").option("header", True).csv(f"{hdfs_base_path}/cancellation_rate_by_airport")


cxl_by_month = (df.groupBy("MONTH").agg(count("*").alias("Total_Flights"),count(when(col("CANCELLED") == 1, True)).alias("Cancelled_Flights")).withColumn("Cancellation_Rate", col("Cancelled_Flights") / col("Total_Flights")).withColumnRenamed("MONTH", "Month"))

cxl_by_month.write.mode("overwrite").option("header", True).csv(f"{hdfs_base_path}/cancellation_rate_by_month")


cxl_reasons = (df.filter(col("CANCELLED") == 1).groupBy("CANCELLATION_CODE").agg(count("*").alias("Count")).withColumnRenamed("CANCELLATION_CODE", "CancellationCode"))

cxl_reasons.write.mode("overwrite").option("header", True).csv(f"{hdfs_base_path}/cancellation_reasons")


common_divert = (df.filter(col("DIVERTED") == 1).groupBy("DIV1_AIRPORT").agg(count("*").alias("Diversions")).withColumnRenamed("DIV1_AIRPORT", "Div1Airport"))

common_divert.write.mode("overwrite").option("header", True).csv(f"{hdfs_base_path}/common_diversion_airports")

divert_by_airline = (df.groupBy("OP_UNIQUE_CARRIER").agg(count("*").alias("Total_Flights"),count(when(col("DIVERTED") == 1, True)).alias("Diverted_Flights")).withColumn("Diversion_Rate", col("Diverted_Flights") / col("Total_Flights")).withColumnRenamed("OP_UNIQUE_CARRIER", "Reporting_Airline"))

divert_by_airline.write.mode("overwrite").option("header", True).csv(f"{hdfs_base_path}/diversion_rate_by_airline")


diversion_impact = (df.filter(col("DIVERTED") == 1).select(avg("ACTUAL_ELAPSED_TIME").alias("Avg_Diverted_Elapsed_Time"),avg("ARR_DELAY").alias("Avg_Diverted_Arrival_Delay"),avg("DISTANCE").alias("Avg_Diverted_Distance")))

diversion_impact.write.mode("overwrite").option("header", True).csv(f"{hdfs_base_path}/diversion_impact")

spark.stop()
