from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, lit, when

spark = SparkSession.builder \
    .appName("Spark Sql Application") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

df = spark.read.option("header", "true").csv("input_customers_data.csv")

#Change data Types of columns
df = df.withColumn("Subscription Date", col("Subscription Date").cast("date")) \
       .withColumn("Year", year(col("Subscription Date"))) \
       .withColumn("Month", month(col("Subscription Date")))

# create a view to perform sql's
df.createOrReplaceTempView("customers")

result1 = spark.sql("SELECT * FROM customers WHERE Country = 'India'")
result1.show()

result2 = spark.sql("SELECT Country, COUNT(*) AS Total_Customers FROM customers GROUP BY Country ORDER BY Total_Customers DESC")
result2.show()

print("---------------------------",df.columns)

new_customer = spark.createDataFrame([
        ("99999", "A1B2C3", "AMBUJ", "Namdev", "Zecdata", "Indore", "India", "123-456-7890", "991-199-9191", 
    "ambuj.n@zecdata.com", "2024-09-03", "https://google.com", 2024, 9)], df.columns)

df = df.union(new_customer)
df.createOrReplaceTempView("customers")

df = df.withColumn("City", when(col("Index") == "99999", lit("Chh")).otherwise(col("City")))
df.createOrReplaceTempView("customers")

df = df.filter(col("Index") != "12")
df.createOrReplaceTempView("customers")

result6 = spark.sql("SELECT City, COUNT(*) AS CustomersPerCity FROM customers GROUP BY City ORDER BY CustomersPerCity DESC")
result6.show()

df.write.mode("overwrite").csv("sql_output")

spark.stop()
