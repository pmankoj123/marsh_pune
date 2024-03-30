# Databricks notebook source


# COMMAND ----------

df=spark.read.csv("dbfs:/FileStore/tables/formula1_raw/circuits.csv")
display(df)

# COMMAND ----------

df=spark.read.option("header",True).option("inferSchema",True).csv("dbfs:/FileStore/tables/formula1_raw/circuits.csv")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Transaformation

# COMMAND ----------

df.select('*').show()

# COMMAND ----------

help(df.select)

# COMMAND ----------

df.select("*").display()

# COMMAND ----------

from pyspark.sql.functions import col
df.select(col("circuitId").alias("circuit_id")).display()

# COMMAND ----------

df.select("circuitId",col("circuitRef"),df.name,df["location"]).display()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df.select(concat("location",lit("&"),"country").alias("loc&country")).display()

# COMMAND ----------

help(df.withColumnRenamed)

# COMMAND ----------

df.withColumnRenamed("circuitId","circuit_Id").withColumnRenamed("name","name1").display()

# COMMAND ----------

help(df.toDF)

# COMMAND ----------

df.columns

# COMMAND ----------

new_columns=['circuitId',
 'circuitRef',
 'name1',
 'location',
 'country',
 'lat',
 'lng',
 'alt',
 'url']

# COMMAND ----------

df1=df.toDF(*new_columns)

# COMMAND ----------

df1.display()

# COMMAND ----------

help(df.drop)

# COMMAND ----------

df2=df1.drop("url")

# COMMAND ----------

display(df2)

# COMMAND ----------

df1.write.mode("overwrite").parquet("dbfs:/FileStore/tables/output/pooja/circuit")

# COMMAND ----------

df1=spark.read.parquet("dbfs:/FileStore/tables/output/pooja/circuit")

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists pooja 

# COMMAND ----------

df1.write.saveAsTable("pooja.circuit")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from pooja.circuit where circuitId=77
# MAGIC union
# MAGIC select * from pooja.circuit where circuitId=10
