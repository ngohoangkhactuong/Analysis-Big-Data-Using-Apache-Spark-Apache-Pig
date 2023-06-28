from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count

# Khởi tạo SparkSession
spark = SparkSession.builder.getOrCreate()

input_path = "hdfs://Master:9000/pig/output/part-r-00000"
output_path = "hdfs://Master:9000/spark/ex2.csv"
# Đọc dữ liệu vào DataFrame
data = spark.read.format("csv").load(input_path)

# Đặt tên cột cho DataFrame
column_names = ["beer_ABV", "beer_beerID", "beer_name", "beer_style", "review_overall"]  # Thay thế bằng danh sách tên cột thực tế
data = data.toDF(*column_names)

A2 = data.select(col("review_overall").cast("float"))

grouped_data = A2.withColumn("range",
    when((col("review_overall") >= 0) & (col("review_overall") < 1), "[0-1]")
    .when((col("review_overall") >= 1) & (col("review_overall") < 2), "[1-2]")
    .when((col("review_overall") >= 2) & (col("review_overall") < 3), "[2-3]")
    .when((col("review_overall") >= 3) & (col("review_overall") < 4), "[3-4]")
    .when((col("review_overall") >= 4) & (col("review_overall") <= 5), "[4-5]")
    .otherwise('Unknown')
).groupBy("range").count().alias("count")

sorted_data = grouped_data.orderBy(col("range"))

sorted_data.write.format("csv").option("header", "true").mode("overwrite").option("path", output_path).save()