from pyspark.sql import SparkSession
from pathlib import Path
from pyspark.sql import functions as f

LOG_FILE_PATH = Path("./web_server_logs.csv")

spark = SparkSession.builder.appName("Analyzer HTTP Logs").getOrCreate()

df = spark.read.csv(LOG_FILE_PATH.as_posix(), header=True, sep=",", inferSchema=True)

df_ip_high_requests = df.groupBy("ip").agg(
    f.count(f.col("ip")).alias("requests_count")
).select("ip", "requests_count").sort(f.col("requests_count").desc())
print("Top 10 active IP addresses:")
df_ip_high_requests.show(10)


df_http_methods = df.groupBy("method").agg(
    f.count(f.col("method")).alias("method_count")
).select("method", "method_count").sort(f.col("method_count").desc())
print("Request count by HTTP method:")
df_http_methods.show()

count_404_response = df.where(
    f.col("response_code") == 404
).count()
print(f"Number of 404 response codes: {count_404_response}")

print()
df_total_response_size_dates = df.withColumn("date", f.to_date(f.col("timestamp"))).groupBy("date").agg(
    f.sum(f.col("response_size")).alias("total_response_size")
).sort("date")
print("Total response size by date:")
df_total_response_size_dates.show()
