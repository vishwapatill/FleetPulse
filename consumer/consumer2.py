from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json,
    col,
    to_timestamp
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType
)


# ============================================================
# SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("TaxiKafkaStreaming")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# KAFKA MESSAGE SCHEMA
# ============================================================

schema = StructType([

    StructField("event_id", StringType(), True),

    StructField("VendorID", DoubleType(), True),

    StructField("lpep_pickup_datetime", StringType(), True),

    StructField("lpep_dropoff_datetime", StringType(), True),

    StructField("passenger_count", DoubleType(), True),

    StructField("trip_distance", DoubleType(), True),

    StructField("RatecodeID", DoubleType(), True),

    StructField("store_and_fwd_flag", StringType(), True),

    StructField("PULocationID", DoubleType(), True),

    StructField("DOLocationID", DoubleType(), True),

    StructField("payment_type", DoubleType(), True),

    StructField("fare_amount", DoubleType(), True),

    StructField("extra", DoubleType(), True),

    StructField("mta_tax", DoubleType(), True),

    StructField("tip_amount", DoubleType(), True),

    StructField("tolls_amount", DoubleType(), True),

    StructField("improvement_surcharge", DoubleType(), True),

    StructField("total_amount", DoubleType(), True),

    StructField("congestion_surcharge", DoubleType(), True),

    StructField("Airport_fee", DoubleType(), True),

    StructField("total_time_in_sec", DoubleType(), True),

    StructField("ptime", StringType(), True),

    StructField("dtime", StringType(), True)
])


# ============================================================
# READ FROM KAFKA
# ============================================================

raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "orders")
    .option("startingOffsets", "latest")
    .load()
)


# ============================================================
# KAFKA VALUE -> STRING
# ============================================================

json_stream = raw_stream.select(
    col("value").cast("string").alias("json")
)


# ============================================================
# PARSE JSON
# ============================================================

parsed_stream = json_stream.select(
    from_json(
        col("json"),
        schema
    ).alias("data")
).select("data.*")


# ============================================================
# CONVERT TIMESTAMPS
# ============================================================

parsed_stream = (
    parsed_stream
    .withColumn(
        "ptime",
        to_timestamp("ptime")
    )
    .withColumn(
        "dtime",
        to_timestamp("dtime")
    )
)


# ============================================================
# DEDUPLICATION
# ============================================================

parsed_stream = parsed_stream.dropDuplicates(
    ["event_id"]
)


# ============================================================
# WRITE TO CONSOLE
# ============================================================

query = (
    parsed_stream
    .writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .option("numRows", 10)
    .start()
)


query.awaitTermination()