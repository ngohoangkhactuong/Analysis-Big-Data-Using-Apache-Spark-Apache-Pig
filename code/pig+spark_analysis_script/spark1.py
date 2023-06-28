from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, concat_ws, lit, concat

# Create a Spark session
spark = SparkSession.builder.getOrCreate()

input_path = "hdfs://Master:9000/pig/output/part-r-00000"
output_path = "hdfs://Master:9000/spark/ex1.csv"

# Read data into DataFrame
data = spark.read.format("csv").load(input_path)

# Set column names for DataFrame
column_names = ["beer_ABV", "beer_beerID", "beer_name", "beer_style", "review_overall"]
renamedData = data.toDF(*column_names)

# Select the required columns and cast data types
A1 = renamedData.select(
    col("beer_ABV").cast("double"),
    col("beer_style").cast("string"),
    col("review_overall").cast("double")
)

# Group by beer_style
B1 = A1.groupBy("beer_style")

# Process each group in B1
C1_process = B1.applyInPandas(
        lambda group, rows: rows.sort_values("review_overall", ascending=False).head(5),
        schema="beer_ABV double, beer_style string, review_overall double"
    )

# Gom các giá trị từ nhiều hàng vào một hàng
C1_group = C1_process.groupby("beer_style").agg(
    collect_list(
        concat(
            lit("( "), 
            concat_ws(",", C1_process["beer_ABV"], C1_process["beer_style"], C1_process["review_overall"]),
            lit(" )")
        )
    ).alias("groups")
)

C1_tranform = C1_group.withColumn("groups", concat_ws(",", "groups"))

C1 = C1_tranform.select("beer_style", "groups")

# Write the grouped data to CSV file
C1.write.format("csv").option("header", "true").mode("overwrite").option("path", output_path).save()
