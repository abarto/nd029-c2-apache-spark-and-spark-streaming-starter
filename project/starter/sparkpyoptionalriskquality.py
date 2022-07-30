from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, LongType, IntegerType


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


stedi_event_schema = StructType([
    StructField("customer", StringType()),
    StructField("score", StringType()),
    StructField("riskDate", StringType()),
])


rapid_step_test_element_schema = StructType([
    StructField("startTime", LongType()),
    StructField("stopTime", LongType()),
    StructField("testTime", LongType()),
    StructField("totalSteps", IntegerType()),
    StructField("customer", StructType([
        StructField("customerName", StringType()),
        StructField("email", StringType()),
        StructField("phone", StringType()),
        StructField("birthDay", StringType()),
    ]))
])


spark = SparkSession.builder.appName(__name__).getOrCreate()
spark.sparkContext.setLogLevel("WARN")


customer_score_stream_df = spark\
    .readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "kafka:19092")\
    .option("subscribe", "stedi-events")\
    .option("startingOffsets", "earliest")\
    .load()\
    .select(
        "timestamp",
        F.from_json(F.col("value").cast("string"), stedi_event_schema).alias("stedi_event"),
    )\
    .select(
        "timestamp",
        F.expr("stedi_event.customer").alias("customer"),
        F.expr("stedi_event.score").alias("score")
    )


def get_score(time_diffs):
    current_avg = (time_diffs[3] - time_diffs[2]) / 2.0
    previous_avg = (time_diffs[1] - time_diffs[0]) / 2.0

    return round((previous_avg - current_avg) / 1000.0, 2)


get_score_udf = F.udf(lambda a: get_score(a))


rapid_step_test_stream_df = spark\
    .readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "kafka:19092")\
    .option("subscribe", "redis-server")\
    .option("startingOffsets", "earliest")\
    .load()\
    .select(
        "timestamp",
        F.from_json(
            F.col("value").cast("string"),
            redis_message_schema
        ).alias("value")
    )\
    .filter(F.expr("value.key") == "UmFwaWRTdGVwVGVzdA==")\
    .withColumn(
        "value",
        F.from_json(
            F.unbase64(F.expr('value.zSetEntries[0].element')).cast("string"),
            rapid_step_test_element_schema
        )
    )\
    .select(
        "timestamp",
        F.expr("value.startTime").alias("startTime"),
        F.expr("value.stopTime").alias("stopTime"),
        F.expr("value.stopTime - value.startTime").alias("timeDiff"),
        F.expr("value.testTime").alias("testTime"),
        F.expr("value.totalSteps").alias("totalSteps"),
        F.expr("value.customer.email").alias("customer"),
    )\
    .withWatermark("timestamp", "30 seconds")\
    .groupby(
        "customer",
        F.window("timestamp", "5 minutes")
    )\
    .agg(
        F.last("stopTime"),
        F.collect_list("timeDiff")
    )\
    .filter(F.size(F.col("collect_list(timeDiff)")) >= 4)\
    .withColumn("time_diffs", F.slice(F.col("collect_list(timeDiff)"), -4, 4))\
    .withColumn("rapidStepTestScore", get_score_udf(F.col("time_diffs")))\
    .select(
        F.col("customer").alias("rapidStepTestCustomer"),
        "rapidStepTestScore",
    )


customer_score_stream_df.join(
    rapid_step_test_stream_df,
    on=customer_score_stream_df.customer == rapid_step_test_stream_df.rapidStepTestCustomer
)\
    .withColumn("isScoreCorrect", F.col("rapidStepTestScore") != F.col("score"))\
    .writeStream\
    .format("console")\
    .start()\
    .awaitTermination()
