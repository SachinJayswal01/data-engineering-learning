# Databricks notebook source
data = [(1, 'Sachin', 'India'), (2, 'Manish', 'San Francisco'), (3, 'Manisha', 'San Francisco')]
schema = ["id","name","location"]
df = spark.createDataFrame(data, schema)


# COMMAND ----------

df.show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType
schema = StructType([ \
    StructField("id", IntegerType()), \
    StructField("name", StringType()), \
    StructField("location", StringType())])
df1 = spark.createDataFrame(data,schema)

# COMMAND ----------

df1.show()

# COMMAND ----------

df1.printSchema()

# COMMAND ----------

