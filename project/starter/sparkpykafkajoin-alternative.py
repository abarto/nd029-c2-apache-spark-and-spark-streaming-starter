from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, regexp_extract, struct
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType


redis_message_schema = StructType([
    StructField("key", StringType()),
    StructField("value", StringType()),
    StructField("expiredType", StringType()),
    StructField("expiredValue",StringType()),
    StructField("existType", StringType()),
    StructField("ch", StringType()),
    StructField("incr", BooleanType()),
    StructField("zSetEntries", ArrayType(
        StructType([
            StructField("element", StringType()),
            StructField("score", StringType())
        ]))
    )
])


customer_schema = StructType([
    StructField("customerName", StringType()),
    StructField("email", StringType()),
    StructField("phone", StringType()),
    StructField("birthDay", StringType()),
])


stedi_event_schema = StructType([
    StructField("customer", StringType()),
    StructField("score", StringType()),
    StructField("riskDate", StringType()),
])


spark = SparkSession.builder.appName(__name__).getOrCreate()
spark.sparkContext.setLogLevel("WARN")


customer_stream_df = spark\
    .readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "kafka:9092")\
    .option("subscribe", "redis-server")\
    .option("startingOffsets", "earliest")\
    .load()\
    .select(
        from_json(col("value").cast("string"),redis_message_schema).alias("value"),
    )\
    .select(
        from_json(
            unbase64(col('value.zSetEntries[0].element').cast("string")),
            customer_schema
        ).alias("customer")
    )\
    .filter(col("customer.birthDay").isNotNull())\
    .select(
        col("customer.email").alias("email"),
        regexp_extract("customer.birthDay", "^(\d{4})-.*$", 1).alias("birthYear")
    )


customer_score_stream_df = spark\
    .readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "kafka:9092")\
    .option("subscribe", "stedi-events")\
    .option("startingOffsets", "earliest")\
    .load()\
    .select(
        from_json(col("value").cast("string"), stedi_event_schema).alias("stedi_event"),
    )\
    .select(
        col("stedi_event.customer").alias("customer"),
        col("stedi_event.score").alias("score"),
    )

customer_stream_df.join(
        customer_score_stream_df,
        on=customer_stream_df.email == customer_score_stream_df.customer
    )\
    .select(
        col("customer").cast("string").alias("key"),
        to_json(struct("*")).alias("value")
    )\
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092")\
    .option("topic", "customer-risk")\
    .option("checkpointLocation","/tmp/kafkacheckpoint")\
    .start()\
    .awaitTermination()
