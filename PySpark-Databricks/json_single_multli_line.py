# Databricks notebook source
df = spark.read.json("/Volumes/workspace/default/pyspark/single.json")

# COMMAND ----------

df.show()

# COMMAND ----------

df1 = spark.read.json("/Volumes/workspace/default/pyspark/multiline.json", multiLine=True)

# COMMAND ----------

df1.show()

# COMMAND ----------

