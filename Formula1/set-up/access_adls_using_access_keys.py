# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Data Lake using access keys 
# MAGIC 1. set the spark config fs.azure.account.key
# MAGIC 1. list files from demo conatiner 
# MAGIC 1. Read data from circuits.csv file 
# MAGIC
# MAGIC

# COMMAND ----------

formula1_account_key=dbutils.secrets.get(scope='fromula1-scope',key='formula1-account-key')

# COMMAND ----------

spark.conf.set("fs.azure.account.key.databricksdl2410.dfs.core.windows.net", formula1_account_key)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@databricksdl2410.dfs.core.windows.net/"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@databricksdl2410.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


