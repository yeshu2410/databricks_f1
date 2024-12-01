# Databricks notebook source
# MAGIC %md 
# MAGIC #### Read the csv file using the spark dataframe reader

# COMMAND ----------

circuit_df =spark.read.option('header',True).csv("dbfs:/mnt/databricksdl2410/raw/circuits.csv")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType 

# COMMAND ----------

circuit_df.printSchema()

# COMMAND ----------

##circuit_df =spark.read.option('header',True).option('inferSchema',True).csv("dbfs:/mnt/databricksdl2410/raw/circuits.csv")
circuit_schema=StructType([StructField("circuitId",IntegerType(),False),
                           StructField("circuitRef",StringType(),True),
                           StructField("name",StringType(),True),
                           StructField("location",StringType(),True),
                           StructField("country",StringType(),True),
                           StructField("lat",DoubleType(),True),
                           StructField("lng",DoubleType(),True),
                           StructField("alt",IntegerType(),True),
                           StructField("url",StringType(),True)])

# COMMAND ----------

circuit_df =spark.read\
    .option('header',True)\
    .schema(circuit_schema).csv("dbfs:/mnt/databricksdl2410/raw/circuits.csv")

# COMMAND ----------

circuit_df.printSchema()

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/databricksdl2410/raw

# COMMAND ----------

circuit_df

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/mnt/databricksdl2410/raw'))

# COMMAND ----------

circuit_df.describe().show()

# COMMAND ----------

circuit_selected_df=circuit_df.select('circuitId','circuitRef', 'name', 'location', 'country', 'lat', 'lng','alt',)

# COMMAND ----------

display(circuit_selected_df)

# COMMAND ----------

from pyspark.sql.functions import col
circuit_df.select(circuit_df['circuitref'],circuit_df.circuitId.alias('race_circuit_id'),col('location')).show()

# COMMAND ----------

circuits_renamed_df=circuit_selected_df.withColumnRenamed('circuitid','circuit_id')\
    .withColumnRenamed('circuitref','circuit_ref')\
    .withColumnRenamed('lat','latitude')\
    .withColumnRenamed('lng','Longitude')\
    .withColumnRenamed('alt','altitude')

# COMMAND ----------

display(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### ingestion of date into the dataframe 
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

circuit_final_df=circuits_renamed_df.withColumn("ingestion_date",current_timestamp())\
    ##.withColumn('testing_lit',lit("that's_working"))


# COMMAND ----------

display(circuit_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC 1. write data to datalake as parquet

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/mnt/databricksdl2410/'))

# COMMAND ----------

circuit_final_df.write.mode('overwrite').parquet('dbfs:/mnt/databricksdl2410/processed/circuits')

# COMMAND ----------

#display(dbutils.fs.ls('dbfs:/mnt/databricksdl2410/processed'))
"""%python
%fs ls /mnt/databricksdl2410/processed/circuits"""

# COMMAND ----------



# COMMAND ----------

df =spark.read.parquet('/mnt/databricksdl2410/processed/circuits')

# COMMAND ----------

display(df)

# COMMAND ----------


