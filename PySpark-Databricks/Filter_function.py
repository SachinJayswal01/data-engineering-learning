# Databricks notebook source
df = spark.read.csv("/Volumes/workspace/default/pyspark/employee.csv",header=True,inferSchema=True)

# COMMAND ----------

df.show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df1 = df.filter(df.name=="rinku")

# COMMAND ----------

df1.show()

# COMMAND ----------

df2 = df.filter(df.address!="india")

# COMMAND ----------

df2.show()

# COMMAND ----------

df3 = df.filter((df.name=="rinku") & (df.emp_id=="3"))

# COMMAND ----------

df3.show()

# COMMAND ----------

df4 = df.filter((df.name=="rani") | (df.address=="india"))

# COMMAND ----------

df4.show()

# COMMAND ----------

df5 = df.filter(df.name.startswith("m"))

# COMMAND ----------

df5.show()

# COMMAND ----------

df6 = df.filter(df.name.endswith("u"))

# COMMAND ----------

df6.show()

# COMMAND ----------

df.show()

# COMMAND ----------

    df7 = df.filter(df.name.like("%i%"))

# COMMAND ----------

df7.show()

# COMMAND ----------

