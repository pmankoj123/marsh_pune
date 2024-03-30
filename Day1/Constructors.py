# Databricks notebook source
# MAGIC %fs ls dbfs:/FileStore/tables/formula1_raw/

# COMMAND ----------

df=spark.read.json('dbfs:/FileStore/tables/formula1_raw/constructors.json')

# COMMAND ----------

df.show()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df1=df.drop("url")

# COMMAND ----------

df1.display()

# COMMAND ----------

df1=df1.withColumn("ingestion_time",current_date())

# COMMAND ----------

df1.write.saveAsTable('pooja.constructor')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from pooja.constructor

# COMMAND ----------

# MAGIC %sql
# MAGIC create table pooja.constructor4 as
# MAGIC select *,current_timestamp() as ingestion_time from json.`dbfs:/FileStore/tables/formula1_raw/constructors.json`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from pooja.constructor3

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM csv.`dbfs:/FileStore/tables/formula1_raw/circuits.csv`OPTIONS header 'true', inferSchema 'true')
