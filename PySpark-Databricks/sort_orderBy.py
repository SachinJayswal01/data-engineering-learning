# Databricks notebook source
df = spark.read.csv("/Volumes/workspace/default/pyspark/employee.csv", header=True, inferSchema=True)

# COMMAND ----------

df.show()

# COMMAND ----------

df1 = df.sort("salary")

# COMMAND ----------

df1.show()

# COMMAND ----------

df2 = df.sort("name")

# COMMAND ----------

df2.show()

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

df3 = df.sort(col("address").desc())

# COMMAND ----------

df3.show()

# COMMAND ----------

df4 = df.orderBy("address")

# COMMAND ----------

df4.show()

# COMMAND ----------

df5 = df.orderBy(col("name").desc())

# COMMAND ----------

df5.show()

# COMMAND ----------

df6 = df.orderBy(col("name").asc())

# COMMAND ----------

df6.show()

# COMMAND ----------

