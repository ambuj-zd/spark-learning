from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("RDDtxtProcessing").getOrCreate()

def starts(file_name, output_fldr):
    rdd = spark.sparkContext.textFile(file_name)

    print("\t\t\t -----------------> First 5 lines of the file:")
    print(rdd.take(5))

    rdd_upper = rdd.map(lambda line: line.upper())

    print("\t\t\t\t --->>>> Uppercased Lines:")
    print(rdd_upper.collect())

    word_counts = (
        rdd.flatMap(lambda line: line.split())
           .map(lambda word: (word, 1))
           .reduceByKey(lambda a, b: a + b)
    )

    print("\t\t\t\t --->>>>  Word Count Results:")
    print(word_counts.collect())

    rdd_upper.saveAsTextFile(f"{output_fldr}/upper_text")
    word_counts.saveAsTextFile(f"{output_fldr}/word_counts")

    print(f"Processing complete. Output saved to '{output_fldr}/processed_text'.")

    rdd_non_empty = rdd.filter(lambda line: line.strip() != "")  # For removing empty lines from dataset

    rdd_words = rdd.flatMap(lambda line: line.split(" "))
    rdd_word_counts = rdd_words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

    sorted_word_counts = rdd_word_counts.sortBy(lambda x: x[1], ascending=False)

    most_frequent_word = sorted_word_counts.first()
    print(f"Most Frequent Word: {most_frequent_word}")


    header = rdd.first()  
    num_columns = len(header.split(","))

    structured_rdd = rdd.filter(lambda line: len(line.split(",")) == num_columns)

    if structured_rdd.count() > 0:
        df = spark.createDataFrame(structured_rdd.map(lambda x: x.split(",")))
        df.write.csv(f"{output_fldr}/structured", header=True, mode="overwrite")
        print(f"Structured data saved as CSV in: txt_output")


file_name1 = "input_txt_data.txt"
file_name2 = "input_unstr_txt_data.txt"
starts(file_name1, "txt_output_1")
starts(file_name2, "txt_output_2")

spark.stop()