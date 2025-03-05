from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, max, min, sum

spark = SparkSession.builder \
        .appName("WithDiffConfigSpark") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.cores", "2") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
        .config("spark.ui.enabled", "true") \
        .config("spark.ui.port", "8001") \
        .getOrCreate()

df = spark.read.csv("input_csv_data.csv", header=True, inferSchema=True)

df.show()

print("Schema of the dataset:")
df.printSchema()

print("Total Rows in Dataset:", df.count())

avg_salary_df = df.groupBy("department").agg(avg(col("salary")).alias("avg_salary"))
avg_salary_df.show()

emp_count_df = df.groupBy("department").agg(count("*").alias("total_employees"))
emp_count_df.show()

salary_stats_df = df.groupBy("department").agg(
    max("salary").alias("max_salary"),
    min("salary").alias("min_salary")
)
salary_stats_df.show()

total_salary_df = df.groupBy("department").agg(sum("salary").alias("total_salary_expenditure"))
total_salary_df.show()

highest_paid_employee = df.orderBy(col("salary").desc()).limit(1)
highest_paid_employee.show()

high_earners = df.filter(col("salary") > 100000)
high_earners.show()

avg_salary_role_df = df.groupBy("job_role").agg(avg("salary").alias("avg_salary"))
avg_salary_role_df.show()

avg_salary_df.write.mode("overwrite").csv("par_output/avg_salary")
emp_count_df.write.mode("overwrite").csv("par_output/emp_count")
salary_stats_df.write.mode("overwrite").csv("par_output/salary_stats")
total_salary_df.write.mode("overwrite").csv("par_output/total_salary")

import time
time.sleep(300)


spark.stop()

#Partitioning-Repartitioning...
#Colesc