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

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}


# COMMAND ----------



# COMMAND ----------

def mount_formula1dl(storage_name,contanier):
  client_id=dbutils.secrets.get(scope='fromula1-scope',key='formula1-app-client-id')
  tenant_id=dbutils.secrets.get(scope='fromula1-scope',key='formula1-app-tenant-id')
  client_secret = dbutils.secrets.get(scope='fromula1-scope',key='formula1-app-secrete-key')
  configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
  if any(mount.mountPoint == f"/mnt/{storage_name}/{contanier}" for mount in dbutils.fs.mounts()):
    dbutils.fs.unmount(f"/mnt/{storage_name}/{contanier}")
  dbutils.fs.mount(
  source = f"abfss://{contanier}@{storage_name}.dfs.core.windows.net/",
  mount_point = f"/mnt/{storage_name}/{contanier}",
  extra_configs = configs)
  display(dbutils.fs.mounts())



# COMMAND ----------

mount_formula1dl("databricksdl2410","raw")

# COMMAND ----------

mount_formula1dl("databricksdl2410","processed")

# COMMAND ----------

mount_formula1dl("databricksdl2410","presentation")

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@databricksdl2410.dfs.core.windows.net/",
  mount_point = "/mnt/formula1dl/demo",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula1dl/demo"))

# COMMAND ----------

display(spark.read.csv("/mnt/formula1dl/demo/circuits.csv"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.fs.unmount("/mnt/formula1dl/demo")

# COMMAND ----------


