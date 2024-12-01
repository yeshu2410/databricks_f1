# Databricks notebook source
race_df=spark.read.option('header',True).csv('dbfs:/mnt/databricksdl2410/raw/races.csv')

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/mnt/databricksdl2410/raw/races.csv
# MAGIC

# COMMAND ----------

display(race_df)

# COMMAND ----------

race_df.printSchema()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType,IntegerType,DoubleType,DateType,TimestampType
race_scehma = StructType([StructField('raceId',StringType(),False),
                         StructField('Year',DateType(),True),
                         StructField('round',IntegerType(),True),
                         StructField('circuitId',IntegerType(),True),
                         StructField('name',StringType(),True),
                         StructField('date',DateType(),True),
                         StructField('time',StringType(),True),
                         StructField('url',StringType(),True)])

# COMMAND ----------

race_df=spark.read.option('header',True).schema(race_scehma).csv('dbfs:/mnt/databricksdl2410/raw/races.csv')

# COMMAND ----------

display(race_df)

# COMMAND ----------

race_df.printSchema()

# COMMAND ----------

race_selected_df=race_df.select('raceId',race_df['Year'],race_df.round,race_df.circuitId,race_df.name,race_df.date,race_df.time)

# COMMAND ----------

race_renamed_df=race_selected_df.withColumnRenamed('raceId','race_Id')\
    .withColumnRenamed('Year','race_Year')\
    .withColumnRenamed('round','race_round')\
    .withColumnRenamed('circuitId','circuit_id')
display(race_renamed_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit,concat,to_timestamp,col
race_date_df=race_renamed_df.withColumn('ingested_time',current_timestamp())\
    .withColumn('race_timestamp',to_timestamp(concat(col('date'),lit(' '),col('time')),'yyyy-MM-dd HH:mm:ss'))
display(race_date_df)

# COMMAND ----------

race_final_df=race_date_df.select(race_date_df['race_Id'],race_date_df['race_Year'].alias('race_year'),'race_round', 'circuit_id','name',race_date_df['ingested_time'].alias('ingested_date'),'race_timestamp')

# COMMAND ----------

display(race_final_df)

# COMMAND ----------

race_final_df.write.mode('overwrite').parquet('mnt/databricksdl2410/processed/race')

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/mnt/databricksdl2410/processed/race

# COMMAND ----------

#display(spark.read.parquet('dbfs:/mnt/databricksdl2410/processed/race'))

# COMMAND ----------


