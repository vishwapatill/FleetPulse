from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("TaxiTrans")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0"
    )
    .getOrCreate()
)

df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "orders")
    .option("startingOffsets", "earliest")
    .load()
)

df.selectExpr(
    "CAST(value AS STRING) as value"
).writeStream \
    .format("console") \
    .outputMode("append") \
    .start() \
    .awaitTermination()