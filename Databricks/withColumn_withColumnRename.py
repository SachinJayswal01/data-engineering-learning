# Databricks notebook source
df = spark.read.csv("/Volumes/workspace/default/pyspark/employee.csv", header=True)

# COMMAND ----------

df.show()

# COMMAND ----------

df1 = df.withColumn("salary1",df.salary*3)

# COMMAND ----------

df1.show()

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

df2 = df.withColumn("country", lit("India"))

# COMMAND ----------

df2.show()

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df3 = df.withColumn("salary",col("salary").cast("int"))

# COMMAND ----------

df3.printSchema()

# COMMAND ----------

df4 = df.withColumnRenamed("loc","location")

# COMMAND ----------

df4.show()