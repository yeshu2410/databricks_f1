# Databricks notebook source
# MAGIC %fs
# MAGIC ls mnt/databricksdl2410/raw

# COMMAND ----------

from pyspark.sql.types import StructType,StringType,IntegerType,StructField,FloatType

pit_stop_schema=StructType([StructField('raceId',IntegerType()),
                            StructField('driverId',IntegerType()),
                            StructField('lap',IntegerType()),
                            StructField('duration',StringType()),
                            StructField('milliseconds',IntegerType()),
                            StructField('stop',IntegerType()),
                            StructField('time',StringType())
])

# COMMAND ----------

pit_stop_df=spark.read.schema(pit_stop_schema).option('multiLine',True).json('dbfs:/mnt/databricksdl2410/raw/pit_stops.json')

# COMMAND ----------



# COMMAND ----------

display(pit_stop_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
pit_renamed_df=pit_stop_df.withColumnRenamed('raceId','race_id')\
                          .withColumnRenamed('driverId','driver_id')\
                            .withColumn('ingestion_date',current_timestamp())

# COMMAND ----------

display(pit_renamed_df)

# COMMAND ----------

pit_renamed_df.write.mode('overwrite').parquet('/mnt/databricksdl2410/processed/pit_stops')

# COMMAND ----------

display(spark.read.parquet('/mnt/databricksdl2410/processed/pit_stops'))

# COMMAND ----------


