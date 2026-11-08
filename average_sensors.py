from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, from_json, to_timestamp, window
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from configs import kafka_config

topic = "building_sensors"
schema = StructType([
    StructField("id", IntegerType()),
    StructField("temperature", DoubleType()),
    StructField("humidity", DoubleType()),
    StructField("timestamp", StringType()),
])

spark = (
    SparkSession.builder.appName("SensorAverages")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.3.0")
    .getOrCreate()
)

username = kafka_config["username"]
password = kafka_config["password"]
jaas = (
    "org.apache.kafka.common.security.plain.PlainLoginModule required "
    f'username="{username}" password="{password}";'
)

sensors = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", ",".join(kafka_config["bootstrap_servers"]))
    .option("subscribe", topic)
    .option("startingOffsets", "latest")
    .option("kafka.security.protocol", kafka_config["security_protocol"])
    .option("kafka.sasl.mechanism", kafka_config["sasl_mechanism"])
    .option("kafka.sasl.jaas.config", jaas)
    .load()
    .select(from_json(col("value").cast("string"), schema).alias("data"))
    .select("data.*")
    .withColumn("timestamp", to_timestamp("timestamp"))
)

averages = (
    sensors.withWatermark("timestamp", "10 seconds")
    .groupBy(window("timestamp", "1 minute", "30 seconds"))
    .agg(
        avg("temperature").alias("average_temperature"),
        avg("humidity").alias("average_humidity"),
    )
)

conditions = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv("alerts_conditions.csv")
)

alerts = (
    averages.crossJoin(conditions)
    .filter(
        ((col("humidity_min") != -999) & col("average_humidity").between(
            col("humidity_min"), col("humidity_max")
        ))
        | ((col("temperature_min") != -999) & col("average_temperature").between(
            col("temperature_min"), col("temperature_max")
        ))
    )
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "average_temperature",
        "average_humidity",
        col("code").alias("alert_code"),
        "message",
    )
)

query = (
    alerts.writeStream.outputMode("append")
    .format("console")
    .option("truncate", False)
    .option("checkpointLocation", "./checkpoint/average_sensors")
    .start()
)

query.awaitTermination()
