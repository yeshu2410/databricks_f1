# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Data Lake using Service Principle 
# MAGIC
# MAGIC 1. Register Azure AD application/Service principle 
# MAGIC 1. Generate a Spark/Password for the application
# MAGIC 1. set spark config with app/clinebt Id, Directory/Tenant Id & Secret
# MAGIC 1. Assign role 'Storage Blob Data Contributor' to the data lake.
# MAGIC
# MAGIC

# COMMAND ----------

client_id=dbutils.secrets.get(scope='fromula1-scope',key='formula1-app-client-id')
tenant_id=dbutils.secrets.get(scope='fromula1-scope',key='formula1-app-tenant-id')
client_secret = dbutils.secrets.get(scope='fromula1-scope',key='formula1-app-secrete-key')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.databricksdl2410.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.databricksdl2410.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.databricksdl2410.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.databricksdl2410.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.databricksdl2410.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@databricksdl2410.dfs.core.windows.net/"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@databricksdl2410.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


