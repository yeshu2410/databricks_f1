# Databricks notebook source
from pyspark.sql.types import  StructType, StructField,StringType,IntegerType,DoubleType,FloatType
results_schema = StructType([StructField("constructorId",IntegerType(),True),
                             StructField("driverId",IntegerType(),True),
                             StructField("fastestLap",IntegerType(),True),
                             StructField("fastestLapSpeed",FloatType(),True),
                             StructField("fastestLapTime",StringType(),True),
                             StructField("grid",IntegerType(),True),
                             StructField("laps",IntegerType(),True),
                             StructField("milliseconds",IntegerType(),True),
                             StructField("number",IntegerType(),True),
                             StructField("points",FloatType(),True),
                             StructField("position",IntegerType(),True),
                             StructField("positionOrder",IntegerType(),True),
                             StructField("positionText",StringType(),True),
                             StructField("raceId",IntegerType(),True),
                             StructField("rank",IntegerType(),True),
                             StructField("resultId",IntegerType(),False),
                             StructField("statusId",StringType(),True),
                            StructField("time",StringType(),True),

])

# COMMAND ----------

results_df=spark.read.schema(results_schema).json('dbfs:/mnt/databricksdl2410/raw/results.json')

# COMMAND ----------

results_df.printSchema()

# COMMAND ----------

display(results_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
results_renamed_df= results_df.withColumnRenamed('resultId','result_id')\
                            .withColumnRenamed('raceId','race_id')\
                            .withColumnRenamed('constructorId','constructor_id')\
                            .withColumnRenamed('driverId','driver_id')\
                            .withColumnRenamed('positionText','position_text')\
                            .withColumnRenamed('positionOrder','position_order')\
                            .withColumnRenamed('fastestLap','fastest_lap')\
                            .withColumnRenamed('fastestLapSpeed','fastest_lap_speed')\
                            .withColumnRenamed('fastestLapTime','fastest_lap_time')\
                            .withColumn('ingested_date', current_timestamp())

# COMMAND ----------

result_final_df= results_renamed_df.drop('statusId')

# COMMAND ----------

display(result_final_df)

# COMMAND ----------

result_final_df.write.mode('overwrite').partitionBy('race_id').parquet('mnt//databricksdl2410/processed/results')

# COMMAND ----------

# MAGIC %fs
# MAGIC ls mnt/databricksdl2410/raw

# COMMAND ----------

display(spark.read.parquet('/mnt//databricksdl2410/processed/results'))

# COMMAND ----------


