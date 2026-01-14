# Databricks notebook source
#create delta table
df_csv= spark.read.format("csv").option("header","true").option("inferSchema","true").load("/Volumes/dev/club_db/data/Employees.csv")


# COMMAND ----------

# DBTITLE 1,Cell 2
#wrting the deltable
df_csv.write.format("delta").mode("overwrite").save("/Volumes/dev/club_db/data/Employees_delta")

# COMMAND ----------

#reading the delta table
df_delta=spark.read.format("delta").load("/Volumes/dev/club_db/data/Employees_delta")
df_delta.printSchema()

# COMMAND ----------

# DBTITLE 1,Cell 4
#Reading the delta logs file
df_json=spark.read.json("/Volumes/dev/club_db/data/Employees_delta/_delta_log/00000000000000000003.json")
display(df_json)

# COMMAND ----------

# MAGIC %sql
# MAGIC /*reading delta file using SQL API*/
# MAGIC select * from delta.`/Volumes/dev/club_db/data/Employees_delta`

# COMMAND ----------

# MAGIC %sql
# MAGIC /*updating the deltal file using SQL API*/
# MAGIC
# MAGIC update delta.`/Volumes/dev/club_db/data/Employees_delta` set salary = 100000 where id = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC /*deleting the deltal file using SQL API*/
# MAGIC
# MAGIC delete from delta.`/Volumes/dev/club_db/data/Employees_delta` where id = 10

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history delta.`/Volumes/dev/club_db/data/Employees_delta`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`/Volumes/dev/club_db/data/Employees_delta` version as of 0 where id=1
# MAGIC union all
# MAGIC select * from delta.`/Volumes/dev/club_db/data/Employees_delta` version as of 5
# MAGIC where id=1
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC /*Restore the previews version of the delta table using SQL API*/
# MAGIC RESTORE TABLE delta.`/Volumes/dev/club_db/data/Employees_delta` TO VERSION AS OF 0

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`/Volumes/dev/club_db/data/Employees_delta` version as of 0 where id=1
# MAGIC union all
# MAGIC select * from delta.`/Volumes/dev/club_db/data/Employees_delta` version as of 6
# MAGIC where id=1
# MAGIC
# MAGIC

# COMMAND ----------

#Creating anoter dataframe for merge shechema case 
#note:data type should be same for merge schema

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.appName("AddDepartmentColumn").getOrCreate()

data = [
    (1, "JOHN", "LONDON", 31330),
    (2, "JACK", "SYDNEY", 154096),
    (3, "SAM", "NEWYORK", 48865),
    (4, "RAKESH", "DELHI", 48594),
    (5, "AJAY", "MUMBAI", 144471)
]

# Create DataFrame (Spark may infer id as long)
df_new_emp = spark.createDataFrame(data, ["id", "name", "city", "salary"])

# Cast id to integer
df_new_emp = df_new_emp.withColumn("id", F.col("id").cast(IntegerType()))
df_new_emp = df_new_emp.withColumn("salary", F.col("id").cast(IntegerType()))

# Add department based on city
df_new_emp = df_new_emp.withColumn(
    "department",
    F.when(F.col("city") == "LONDON", F.lit("HR"))
     .when(F.col("city") == "SYDNEY", F.lit("Finance"))
     .when(F.col("city") == "NEWYORK", F.lit("IT"))
     .when(F.col("city") == "DELHI", F.lit("Admin"))
     .otherwise(F.lit("Others"))
)

# Check schema and show
df_new_emp.printSchema()
df_new_emp.show()


# COMMAND ----------

df_delta.printSchema()

# COMMAND ----------

df_new_emp.printSchema()

# COMMAND ----------

(df_new_emp.write.format('delta').mode("append").option("mergeSchema",True).option("path","/Volumes/dev/club_db/data/Employees_delta").save())

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`/Volumes/dev/club_db/data/Employees_delta`

# COMMAND ----------


