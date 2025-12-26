# Databricks notebook source
import pyspark
from pyspark.sql import SparkSession

# COMMAND ----------

emp = [("James","Sales","NY",90000, 34,10000), \
    ("Michael","Sales","NY",86000,56,20000), \
    ("Robert","Sales","CA",81000,30,23000), \
    ("Maria","Finance","CA",90000,24,23000) ]

columns = ["name","department","state","salary","age","bonus"]
df1 = spark.createDataFrame(data=emp, schema = columns)
df1.show()

# COMMAND ----------

emp1 = [("James","Sales","NY",90000, 34,10000), \
    ("Michael","Sales","NY",86000,56,20000), \
    ("Robert","Sales","CA",81000,30,23000), \
    ("Abhishek","IT","NY",20000000,10,0),
    ("Maria","Finance","CA",90000,24,23000)]

columns = ["name","department","state","salary","age","bonus"]
df2 = spark.createDataFrame(data=emp1, schema = columns)
df2.show()

# COMMAND ----------

df1.printSchema()
df2.printSchema()

# COMMAND ----------

df1.union(df2).show()

# COMMAND ----------

df1.union(df2).distinct().show()

# COMMAND ----------

df1.unionAll(df2).show()

# COMMAND ----------

