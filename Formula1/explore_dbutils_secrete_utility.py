# Databricks notebook source
dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope ='fromula1-scope')

# COMMAND ----------

dbutils.secrets.get(scope='fromula1-scope',key='formula1-account-key')

# COMMAND ----------


