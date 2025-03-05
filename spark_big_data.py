from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, count

spark = SparkSession.builder \
    .appName("CustomerDataProcessing") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "2g") \
    .config("spark.ui.enabled", "true") \
    .config("spark.ui.port", "8002") \
    .getOrCreate()

df = spark.read.option("header", True).csv("input_customers_data.csv")

df = df.withColumn("Subscription Date", col("Subscription Date").cast("date"))

df = df.withColumn("Subscription Year", year(col("Subscription Date"))) \
       .withColumn("Subscription Month", month(col("Subscription Date")))

#Customers per country
country_count_df = df.groupBy("Country").agg(count("*").alias("Customer Count"))

df.show()
country_count_df.show()

df.write.mode("overwrite").parquet("big_d_output/customer_data.parquet")
country_count_df.write.mode("overwrite").parquet("output/customer_count.parquet")



import time
time.sleep(300) #use 5 min of sleep to see UI before it ended
spark.stop()
