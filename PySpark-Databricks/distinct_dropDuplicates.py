# Databricks notebook source
df = spark.read.csv("/Volumes/workspace/default/pyspark/employee.csv", header=True, inferSchema=True)

# COMMAND ----------

df.show()

# COMMAND ----------

df1 = df.distinct()

# COMMAND ----------

df1.show()

# COMMAND ----------

df2 = df.dropDuplicates(["salary","emp_id"])

# COMMAND ----------

df2.show()

# COMMAND ----------

df3 = df.dropDuplicates(["salary"])

# COMMAND ----------

df3.show()

# COMMAND ----------

