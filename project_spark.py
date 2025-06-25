import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, year, month, dayofmonth, hour, broadcast
from pyspark.sql.types import StringType, StructType, StructField, LongType, ArrayType

from util.config import Config
from util.logger import Log4j
from user_agents import parse as parse_ua
from urllib.parse import urlparse

from pyspark.sql.functions import udf


@udf(returnType=StringType())
def get_country(url):
    try:
        domain = urlparse(url).netloc
        return domain.split('.')[-1].upper()
    except:
        return "UNKNOWN"

@udf(returnType=StringType())
def get_browser(ua_string):
    try:
        return parse_ua(ua_string).browser.family
    except:
        return "UNKNOWN"

@udf(returnType=StringType())
def get_os(ua_string):
    try:
        return parse_ua(ua_string).os.family
    except:
        return "UNKNOWN"

@udf(returnType=StringType())
def get_device(ua_string):
    try:
        return parse_ua(ua_string).device.family or "Unknown"
    except:
        return "UNKNOWN"

if __name__ == '__main__':
    conf = Config()
    spark_conf = conf.spark_conf
    kafka_conf = conf.kafka_conf
    pg_url = conf.postgres_url
    pg_props = conf.postgres_properties

    spark = SparkSession.builder \
        .config(conf=spark_conf) \
        .getOrCreate()

    log = Log4j(spark)
    log.info(f"spark_conf: {spark_conf.getAll()}")
    log.info(f"kafka_conf: {kafka_conf.items()}")

    # Kafka schema
    schema = StructType([
        StructField("id", StringType()),
        StructField("api_version", StringType()),
        StructField("collection", StringType()),
        StructField("current_url", StringType()),
        StructField("device_id", StringType()),
        StructField("email", StringType()),
        StructField("ip", StringType()),
        StructField("local_time", StringType()),
        StructField("option", ArrayType(
            StructType([
                StructField("option_id", StringType()),
                StructField("option_label", StringType())
            ])
        )),
        StructField("product_id", StringType()),
        StructField("referrer_url", StringType()),
        StructField("store_id", StringType()),
        StructField("time_stamp", LongType()),
        StructField("user_agent", StringType())
    ])

    # Đọc Kafka
    df_kafka = spark.readStream \
        .format("kafka") \
        .options(**kafka_conf) \
        .load()

    df = df_kafka.selectExpr("CAST(value AS STRING)") \
        .withColumn("json", from_json(col("value"), schema)) \
        .select("json.*")

    df_enriched = df \
        .withColumn("local_time_ts", to_timestamp("local_time", "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("country", get_country("current_url")) \
        .withColumn("browser", get_browser("user_agent")) \
        .withColumn("os", get_os("user_agent")) \
        .withColumn("device_type", get_device("user_agent")) \
        .withColumn("year", year("local_time_ts")) \
        .withColumn("month", month("local_time_ts")) \
        .withColumn("day", dayofmonth("local_time_ts")) \
        .withColumn("hour", hour("local_time_ts"))


    def write_to_postgres(batch_df, batch_id):
        batch_df.persist()

        # dim_time
        df_time = batch_df.select("local_time_ts", "year", "month", "day", "hour") \
            .dropDuplicates() \
            .filter(col("local_time_ts").isNotNull()) \
            .withColumnRenamed("local_time_ts", "full_time")
        df_time.write.jdbc(pg_url, "dim_time", "append", properties=pg_props)

        df_time_with_id = spark.read.jdbc(pg_url, "dim_time", properties=pg_props).select("time_id", "full_time")
        batch_df_with_time_id = batch_df \
            .filter(col("local_time_ts").isNotNull()) \
            .join(broadcast(df_time_with_id), batch_df.local_time_ts == df_time_with_id.full_time, how="left") \
            .drop("full_time")

        # dim_user
        df_user = batch_df.selectExpr("device_id as user_id", "email", "ip", "country") \
            .dropDuplicates() \
            .filter(col("user_id").isNotNull())

        existing_user = spark.read.jdbc(pg_url, "dim_user", properties=pg_props).select("user_id").dropna()
        df_user_new = df_user.join(broadcast(existing_user), on="user_id", how="left_anti")
        df_user_new.write.jdbc(pg_url, "dim_user", "append", properties=pg_props)

        # dim_product
        df_product = batch_df.select("product_id") \
            .dropDuplicates() \
            .filter(col("product_id").isNotNull())

        existing_product = spark.read.jdbc(pg_url, "dim_product", properties=pg_props).select("product_id").dropna()
        df_product_new = df_product.join(broadcast(existing_product), on="product_id", how="left_anti")
        df_product_new.write.jdbc(pg_url, "dim_product", "append", properties=pg_props)

        # dim_store
        df_store = batch_df.select("store_id") \
            .dropDuplicates() \
            .filter(col("store_id").isNotNull())

        existing_store = spark.read.jdbc(pg_url, "dim_store", properties=pg_props).select("store_id").dropna()
        df_store_new = df_store.join(broadcast(existing_store), on="store_id", how="left_anti")
        df_store_new.write.jdbc(pg_url, "dim_store", "append", properties=pg_props)

        # dim_device
        df_device = batch_df.select("device_id", "browser", "os", "device_type") \
            .dropDuplicates() \
            .filter(col("device_id").isNotNull())

        existing_device = spark.read.jdbc(pg_url, "dim_device", properties=pg_props).select("device_id").dropna()
        df_device_new = df_device.join(broadcast(existing_device), on="device_id", how="left_anti")
        df_device_new.write.jdbc(pg_url, "dim_device", "append", properties=pg_props)

        # fact_user_logs
        # batch_df_with_time_id.printSchema()
        # batch_df_with_time_id.show(5,truncate = False)
        df_fact = batch_df_with_time_id.selectExpr(
            "id as log_id", "product_id", "store_id",
            "device_id", "time_id", "referrer_url", "current_url",
            "collection", "time_stamp", "device_id as user_id"
        )

        existing_fact = spark.read.jdbc(pg_url, "fact_user_logs", properties=pg_props).select("log_id").dropna()
        df_fact_new = df_fact.join(broadcast(existing_fact), on="log_id", how="left_anti")
        df_fact_new.write.jdbc(pg_url, "fact_user_logs", "append", properties=pg_props)

        batch_df.unpersist()

    # Write Stream to PostgreSQL
    query = df_enriched.writeStream \
        .foreachBatch(write_to_postgres) \
        .outputMode("append") \
        .trigger(processingTime="30 seconds") \
        .start()

    query.awaitTermination()
