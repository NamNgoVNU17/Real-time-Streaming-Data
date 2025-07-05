from pyspark.sql import SparkSession
from util.logger import Log4j
from util.config import Config
from util.udfs import get_browser, get_os, get_device, get_country, extract_domain
from db.schema import kafka_schema
from db.writer import write_to_postgres
import pyspark.sql.functions as f

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

    # Đọc Kafka
    df_kafka = spark.readStream \
        .format("kafka") \
        .options(**kafka_conf) \
        .load()

    df = df_kafka.selectExpr("CAST(value AS STRING)") \
        .withColumn("json", f.from_json(f.col("value"), kafka_schema)) \
        .select("json.*")

    df_enriched = df \
        .withColumn("local_time_ts", f.to_timestamp("local_time", "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("country", get_country("current_url")) \
        .withColumn("browser", get_browser("user_agent")) \
        .withColumn("os", get_os("user_agent")) \
        .withColumn("device_type", get_device("user_agent")) \
        .withColumn("referrer_domain", extract_domain("referrer_url")) \
        .withColumn("time_id", f.md5(f.col("local_time_ts").cast("string"))) \
        .withColumn("year", f.year("local_time_ts")) \
        .withColumn("month", f.month("local_time_ts")) \
        .withColumn("day", f.dayofmonth("local_time_ts")) \
        .withColumn("hour", f.hour("local_time_ts"))

    query = df_enriched.writeStream \
        .foreachBatch(lambda df, batch_id: write_to_postgres(df, batch_id, spark, pg_url,pg_props)) \
        .outputMode("append") \
        .trigger(processingTime="30 seconds") \
        .start()

    query.awaitTermination()
