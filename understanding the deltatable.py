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
df.show()

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

# MAGIC %sql
# MAGIC /*Schema evoluaiton using SQL API*/
# MAGIC ALTER TABLE delta.`/Volumes/dev/club_db/data/Employees_delta` ADD COLUMNS (bonus int)
# MAGIC     
# MAGIC select * from delta.`/Volumes/dev/club_db/data/Employees_delta`
# MAGIC
