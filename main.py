from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, when

spark = SparkSession.builder \
    .appName("US Flights Pipeline") \
    .enableHiveSupport() \
    .getOrCreate()

df = spark.read.parquet("us_flight_data.parquet")

spark.sql("CREATE DATABASE IF NOT EXISTS flights_db")
spark.sql("USE flights_db")

df.write.mode("overwrite").saveAsTable("raw_flight_data")

busiest_airports = df.groupBy("Origin").count().orderBy(col("count").desc())
busiest_airports.write.mode("overwrite").saveAsTable("busiest_airports")

busiest_airlines = df.groupBy("Reporting_Airline").count().orderBy(col("count").desc())
busiest_airlines.write.mode("overwrite").saveAsTable("busiest_airlines")

busiest_routes = df.groupBy("Origin", "Dest").count().orderBy(col("count").desc())
busiest_routes.write.mode("overwrite").saveAsTable("busiest_routes")

delay_stats = df.groupBy("Reporting_Airline") \
    .agg(avg("DepDelay").alias("AvgDepDelay"),
         avg("ArrDelay").alias("AvgArrDelay"))
delay_stats.write.mode("overwrite").saveAsTable("airline_delay_stats")

delay_percent = df.withColumn("IsDelayed", when(col("ArrDel15") == 1, 1).otherwise(0)) \
    .groupBy("Reporting_Airline") \
    .agg(avg("IsDelayed").alias("DelayPercentage"))
delay_percent.write.mode("overwrite").saveAsTable("airline_delay_percentages")

taxi_times = df.groupBy("Origin") \
    .agg(avg("TaxiOut").alias("AvgTaxiOut"),
         avg("TaxiIn").alias("AvgTaxiIn"))
taxi_times.write.mode("overwrite").saveAsTable("airport_taxi_times")

elapsed_diff = df.withColumn("TimeDiff", col("ActualElapsedTime") - col("CRSElapsedTime")) \
    .groupBy("Reporting_Airline") \
    .agg(avg("TimeDiff").alias("AvgTimeDifference"))
elapsed_diff.write.mode("overwrite").saveAsTable("elapsed_time_comparison")

cancellation_rate = df.withColumn("IsCancelled", when(col("Cancelled") == 1, 1).otherwise(0)) \
    .groupBy("Reporting_Airline") \
    .agg(avg("IsCancelled").alias("CancelRate"))
cancellation_rate.write.mode("overwrite").saveAsTable("airline_cancellation_rate")

diversion_rate = df.withColumn("IsDiverted", when(col("Diverted") == 1, 1).otherwise(0)) \
    .groupBy("Reporting_Airline") \
    .agg(avg("IsDiverted").alias("DivertRate"))
diversion_rate.write.mode("overwrite").saveAsTable("airline_diversion_rate")

top_divert_airports = df.groupBy("Div1Airport").count().orderBy(col("count").desc())
top_divert_airports.write.mode("overwrite").saveAsTable("top_divert_airports")
