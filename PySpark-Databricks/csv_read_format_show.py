# Databricks notebook source
df = spark.read.csv('/Volumes/workspace/default/pyspark/employee3.csv')

# COMMAND ----------

df.show()

# COMMAND ----------

df.printSchema()


# COMMAND ----------

display(df)

# COMMAND ----------

df1 = spark.read.csv('/Volumes/workspace/default/pyspark/employee3.csv', header=True)

# COMMAND ----------

df1.show()

# COMMAND ----------

df1.printSchema()

# COMMAND ----------

df2 = spark.read.csv('/Volumes/workspace/default/pyspark/employee3.csv',header=True,inferSchema=True)

# COMMAND ----------

df2.printSchema()

# COMMAND ----------

df3 = spark.read.format('csv').options(header=True, inferSchema=True).load('/Volumes/workspace/default/pyspark/employee3.csv')

# COMMAND ----------

df3.printSchema()

# COMMAND ----------

