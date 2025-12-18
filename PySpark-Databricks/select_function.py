# Databricks notebook source
df = spark.read.csv("/Volumes/workspace/default/pyspark/employee.csv", header=True, inferSchema=True)

# COMMAND ----------

df.show()

# COMMAND ----------

df.select("name","salary").show()

# COMMAND ----------

df.select(df.emp_id,df.name,df.salary).show()

# COMMAND ----------

df.select(df.columns[0:2]).show()

# COMMAND ----------

