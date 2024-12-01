# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Data Lake using cluster scoped uthrntication 
# MAGIC 1. set the spark config fs.azure.account.key
# MAGIC 1. list files from demo conatiner 
# MAGIC 1. Read data from circuits.csv file 
# MAGIC
# MAGIC

# COMMAND ----------



# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@databricksdl2410.dfs.core.windows.net/"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@databricksdl2410.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


