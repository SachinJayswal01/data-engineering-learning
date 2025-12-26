# Databricks notebook source
emp_df = spark.read.csv("/Volumes/workspace/default/pyspark/emp_2.csv",header=True,inferSchema=True)

# COMMAND ----------

emp_df.show()

# COMMAND ----------

dept_df = spark.read.csv("/Volumes/workspace/default/pyspark/department_1.csv",header=True,inferSchema=True)

# COMMAND ----------

dept_df.show()

# COMMAND ----------

inner_df = emp_df.join(dept_df, emp_df.emp_id==dept_df.user, 'inner')
inner_df.show()

# COMMAND ----------

left_df = emp_df.join(dept_df,emp_df.emp_id==dept_df.user,'left')
left_df.show()

# COMMAND ----------

right_df = emp_df.join(dept_df,emp_df.emp_id==dept_df.user,'right')
right_df.show()

# COMMAND ----------

fullouter_df=emp_df.join(dept_df,emp_df.emp_id==dept_df.user,'fullouter')
fullouter_df.show()


# COMMAND ----------

