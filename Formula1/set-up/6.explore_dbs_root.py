# Databricks notebook source
# MAGIC %md
# MAGIC ### Explore Dbfs root 
# MAGIC 1. list all the folders in DBFS root 
# MAGIC 1. interact with DBFS file Browser
# MAGIC 1. upload file to DBFs root 

# COMMAND ----------

display(dbutils.fs.ls('/'))

# COMMAND ----------

display(dbutils.fs.ls('/FileStore'))

# COMMAND ----------

display(spark.read.csv('/FileStore/circuits.csv'))

# COMMAND ----------


