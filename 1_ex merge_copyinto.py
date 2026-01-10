# Databricks notebook source
# DBTITLE 1,Install mysql-connector-python
# MAGIC %pip install mysql-connector-python

# COMMAND ----------

import pandas as pd
import mysql.connector
from mysql.connector import Error

hostname = "lfq184.h.filess.io"
database = "olistproject_detailnomy"
port = 61002
username = "olistproject_detailnomy"
password = "96f79697740ddfb84a6f58ba58440c96d58024f4"

try:
    conn = mysql.connector.connect(
        host=hostname,
        database=database,
        user=username,
        password=password,
        port=port
    )

    query = "SELECT * FROM olist_order_payments"   # change table name
    df = pd.read_sql(query, conn)

    print(df.head())

except Error as e:
    print("Error:", e)

finally:
    if conn.is_connected():
        conn.close()


# COMMAND ----------

spark_df = spark.createDataFrame(df)
display(spark_df)

# COMMAND ----------


