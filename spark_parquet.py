from pyspark.sql import SparkSession
from pyspark.sql.functions import rank, dense_rank, row_number, avg, count
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("Parquet_File") \
    .getOrCreate()

df = spark.read.parquet("input_pqt_data.parquet")

df.show()

dept_count = df.groupBy("department").agg(count("*").alias("num_employees"))
dept_count.show()

highest_paid = df.orderBy(df.salary.desc()).limit(1)
highest_paid.show()

avg_salary_by_role = df.groupBy("job_role").agg(avg("salary").alias("avg_salary"))
avg_salary_by_role.show()

high_earners = df.filter(df.salary > 100000)
high_earners.show()

salary_expenditure = df.groupBy("department").agg(sum("salary").alias("total_salary"))
salary_expenditure.orderBy(salary_expenditure.total_salary.desc()).show()


# Usage of Windows Function with parquet File
window_spec = Window.partitionBy("department").orderBy(df.salary.desc())

df_rank = df.withColumn("rank", rank().over(window_spec))

df_dense_rank = df.withColumn("dense_rank", dense_rank().over(window_spec))

df_row_number = df.withColumn("row_number", row_number().over(window_spec))

window_spec_avg = Window.partitionBy("department")

df_avg_salary = df.withColumn("avg_salary", avg("salary").over(window_spec_avg))

print("Rank Win Func Result:")
df_rank.show()

print("Dense Rank Window Func Result:")
df_dense_rank.show()

print("Row Number Window Func Result:")
df_row_number.show()

print("Average Salary For each Department:")
df_avg_salary.show()

spark.stop()
