# Databricks notebook source
from pyspark.sql.types import StructType,StructField, IntegerType,StringType, DateType
name_schema=StructType([StructField('forename',StringType(),True),
                        StructField('surname',StringType(),True)
                        ])
driver_schema = StructType([StructField('code',StringType(),True),
                            StructField('driverId',IntegerType(),False),
                            StructField('driverRef',StringType(),True),
                            StructField('name',name_schema,True),
                            StructField('dob',DateType(),True),
                            StructField('nationality',StringType(),True),
                            StructField('number',IntegerType(),True),
                            StructField('url',StringType(),True)

])

# COMMAND ----------

driver_file_df= spark.read.option('header',True).schema(driver_schema).json('/mnt/databricksdl2410/raw/drivers.json')

# COMMAND ----------

driver_file_df.printSchema()

# COMMAND ----------

display(driver_file_df)

# COMMAND ----------

from pyspark.sql.functions import col,lit,current_timestamp,concat
driver_changed_df =driver_file_df.withColumnRenamed('driverId','driver_Id')\
                                .withColumnRenamed('driverRef','driver_Ref')\
                                .withColumn('ingested_date',current_timestamp())\
                                .withColumn('name',concat(col('name.forename'),lit(" "),col('name.surname')))

# COMMAND ----------

display(driver_changed_df)

# COMMAND ----------

driver_final_df =driver_changed_df.drop('url')

# COMMAND ----------

display(driver_final_df)

# COMMAND ----------

driver_final_df.write.mode('overwrite').parquet('/mnt/databricksdl2410/processed/drivers')

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/databricksdl2410/processed/drivers

# COMMAND ----------

display(spark.read.parquet('/mnt/databricksdl2410/processed/drivers'))

# COMMAND ----------


