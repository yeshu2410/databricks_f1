# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Data Lake using access SAS token
# MAGIC
# MAGIC 1. set the spark config fs.azure.account.key
# MAGIC 1. list files from demo conatiner 
# MAGIC 1. Read data from circuits.csv file 
# MAGIC
# MAGIC

# COMMAND ----------

formula1_demo_token=dbutils.secrets.get(scope='fromula1-scope',key='fornula1-sas-key')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.databricksdl2410.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.databricksdl2410.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.databricksdl2410.dfs.core.windows.net",formula1_demo_token)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@databricksdl2410.dfs.core.windows.net/"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@databricksdl2410.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


