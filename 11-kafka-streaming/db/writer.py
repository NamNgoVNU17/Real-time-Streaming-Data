from pyspark.sql.functions import col, broadcast, md5
from util.udfs import upsert_to_postgres

def write_dim_time(df, spark, pg_url, pg_props):
    df_time = df.select("time_id","local_time_ts", "year", "month", "day", "hour") \
        .dropDuplicates(["time_id"]) \
        .filter(col("local_time_ts").isNotNull()) \
        .withColumnRenamed("local_time_ts", "full_time")

    upsert_to_postgres(df_time, "dim_time", ["time_id"], pg_url, pg_props)

def write_dim_user(df, spark, pg_url, pg_props):
    df_user = df.selectExpr("device_id as user_id", "email", "ip", "country") \
        .dropDuplicates(["user_id"]) \
        .filter(col("user_id").isNotNull())

    upsert_to_postgres(df_user, "dim_user", ["user_id"], pg_url, pg_props)


def write_dim_device(df, spark, pg_url, pg_props):
    df_device = df.select("device_id", "browser", "os", "device_type") \
        .dropDuplicates(["device_id"]) \
        .filter(col("device_id").isNotNull())

    upsert_to_postgres(df_device, "dim_device", ["device_id"], pg_url, pg_props)


def write_dim_product(df, spark, pg_url, pg_props):
    df_product = df.select("product_id") \
        .dropDuplicates(["product_id"]) \
        .filter(col("product_id").isNotNull())

    upsert_to_postgres(df_product, "dim_product", ["product_id"], pg_url, pg_props)


def write_dim_store(df, spark, pg_url, pg_props):
    df_store = df.select("store_id") \
        .dropDuplicates(["store_id"]) \
        .filter(col("store_id").isNotNull())

    upsert_to_postgres(df_store, "dim_store", ["store_id"], pg_url, pg_props)


def write_dim_referrer(df, spark, pg_url, pg_props):
    df_ref = df.selectExpr("referrer_url", "referrer_domain as domain") \
        .dropDuplicates(["referrer_url"]) \
        .filter(col("referrer_url").isNotNull())

    upsert_to_postgres(df_ref, "dim_referrer", ["referrer_url"], pg_url, pg_props)


def write_fact_product_views(df, spark, pg_url, pg_props):
    df_fact = df.selectExpr(
        "id as log_id",
        "product_id",
        "store_id",
        "device_id as user_id",
        "device_id",
        "time_id",
        "referrer_url",
        "current_url",
        "country",
        "time_stamp"
    ).dropna(subset=["log_id", "time_id"])

    upsert_to_postgres(df_fact, "fact_product_views", ["log_id"], pg_url, pg_props)




def write_to_postgres(df, batch_id, spark, pg_url, pg_props):
    df.persist()

    write_dim_time(df, spark, pg_url, pg_props)
    write_dim_user(df, spark, pg_url, pg_props)
    write_dim_device(df, spark, pg_url, pg_props)
    write_dim_product(df, spark, pg_url, pg_props)
    write_dim_store(df, spark, pg_url, pg_props)
    write_dim_referrer(df, spark, pg_url, pg_props)
    write_fact_product_views(df, spark, pg_url, pg_props)

    df.unpersist()
