# Databricks notebook source
df = spark.read.csv("/Volumes/workspace/default/pyspark/employee.csv", header=True, inferSchema=True)

# COMMAND ----------

df.show()

# COMMAND ----------

df1 = df.groupBy("address").sum("salary")

# COMMAND ----------

df1.show()

# COMMAND ----------

df2 = df.groupBy("address").count()

# COMMAND ----------

df2.show()

# COMMAND ----------

df3 = df.groupBy("address","emp_id").count()

# COMMAND ----------

df3.show()

# COMMAND ----------

df4 = df.groupBy("address","emp_id").sum("salary")

# COMMAND ----------

df4.show()

# COMMAND ----------

df5 = df.groupBy("address").max("salary")

# COMMAND ----------

df5.show()

# COMMAND ----------

df6 = df.groupBy("address").min("salary")

# COMMAND ----------

df6.show()

# COMMAND ----------

df7 = df.groupBy("address").mean("emp_id")

# COMMAND ----------

df7.show()

# COMMAND ----------

df8 = df.groupBy("address").avg("salary","emp_id")

# COMMAND ----------

df8.show()

# COMMAND ----------

