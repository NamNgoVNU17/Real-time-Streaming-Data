from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from user_agents import parse as parse_ua
from urllib.parse import urlparse
from pyspark.sql import DataFrame
import psycopg2
import time

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
@udf(StringType())
def extract_domain(url):
    try:
        return urlparse(url).netloc
    except:
        return "unknown"


def upsert_to_postgres(df: DataFrame, table_name: str, key_columns: list, pg_url: str, pg_props: dict):
    temp_table = f"temp_{table_name}_{int(time.time())}"
    df.write.jdbc(pg_url, temp_table, mode="overwrite", properties=pg_props)

    # Build UPSERT query
    cols = df.columns
    cols_str = ", ".join(cols)
    values_str = ", ".join([f"t.{c}" for c in cols])
    conflict_cols = ", ".join(key_columns)
    update_set = ", ".join([f"{c} = EXCLUDED.{c}" for c in cols if c not in key_columns])

    upsert_query = f"""
        INSERT INTO {table_name} ({cols_str})
        SELECT {values_str} FROM {temp_table} t
        ON CONFLICT ({conflict_cols})
        DO {f'UPDATE SET {update_set}' if update_set else 'NOTHING'}
    """

    try:
        conn = psycopg2.connect(
            dbname=pg_props.get("dbname"),
            user=pg_props.get("user"),
            password=pg_props.get("password"),
            host=pg_props.get("host"),
            port=pg_props.get("port")
        )
        cursor = conn.cursor()
        cursor.execute(upsert_query)
        cursor.execute(f"DROP TABLE IF EXISTS {temp_table}")
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Error upserting to {table_name}: {str(e)}")
        raise