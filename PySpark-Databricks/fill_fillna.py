# Databricks notebook source
df = spark.read.csv("/Volumes/workspace/default/pyspark/sample.csv",header=True,inferSchema=True)

# COMMAND ----------

df.show()

# COMMAND ----------

df.fillna("other").show()

# COMMAND ----------

df.na.fill("NA").show()

# COMMAND ----------

df.na.fill("Non",["city"]).show()

# COMMAND ----------

df.na.fill(0,["population"]).na.fill("NA",["type"]).show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

