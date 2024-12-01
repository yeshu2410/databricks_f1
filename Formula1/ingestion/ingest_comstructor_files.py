# Databricks notebook source
# MAGIC %md
# MAGIC 1.  step 1- read the json file using the spark dataframe reader 

# COMMAND ----------

constructo_schema='constructorId INT, constructorRef String,name String , nationality String,url String '

# COMMAND ----------

constructor_df =spark.read\
    .schema(constructo_schema)\
    .json('dbfs:/mnt/databricksdl2410/raw/constructors.json')

# COMMAND ----------

constructor_df.printSchema()

# COMMAND ----------

display(constructor_df)

# COMMAND ----------

constructor_dropped_df =constructor_df.drop('url')
#constructor_dropped_df =constructor_df.drop(constructor_df['url'], constructor_df.url)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
constructor_final_df= constructor_dropped_df.withColumnRenamed('constructorId','constructor_id')\
                                             .withColumnRenamed('constructorRef','constructor_ref')\
                                             .withColumn('ingested_date',current_timestamp())

# COMMAND ----------

constructor_final_df.printSchema()

# COMMAND ----------

constructor_final_df.write.mode('overwrite').parquet('/mnt/databricksdl2410/processed/constructors')

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/databricksdl2410/processed/

# COMMAND ----------


