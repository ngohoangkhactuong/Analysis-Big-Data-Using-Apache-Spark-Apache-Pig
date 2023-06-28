from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct

# Khởi tạo SparkSession
spark = SparkSession.builder.getOrCreate()

input_path = "hdfs://Master:9000/pig/output/part-r-00000"
output_path = "hdfs://Master:9000/spark/ex3.csv"
# Đọc dữ liệu vào DataFrame
data = spark.read.format("csv").load(input_path)

# Đặt tên cột cho DataFrame
column_names = ["beer_ABV", "beer_beerID", "beer_name", "beer_style", "review_overall"]  # Thay thế bằng danh sách tên cột thực tế
data = data.toDF(*column_names)

# Chọn các cột cần thiết và truyền kiểu dữ liệu
A3 = data.select(
    col("beer_style").cast("string"),
    col("beer_beerID").cast("int")
)

# Group by beer_style
B3 = A3.groupBy("beer_style")

C3 = B3.agg(countDistinct("beer_beerID").alias("number_beer"))

C3.write.format("csv").option("header", "true").mode("overwrite").option("path", output_path).save()