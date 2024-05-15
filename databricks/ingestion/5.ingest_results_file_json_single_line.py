# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Ingest results.json single line file
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Read the JSON file using the spark dataframe reader API
# MAGIC

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

# define the schema for Results 

results_schema = StructType(fields=[StructField("resultId", IntegerType(), False),
                                    StructField("raceId", IntegerType(), True),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("constructorId", IntegerType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("grid", IntegerType(), True),
                                    StructField("position", IntegerType(), True),
                                    StructField("positionText", StringType(), True),
                                    StructField("positionOrder", IntegerType(), True),
                                    StructField("points", FloatType(), True),
                                    StructField("laps", IntegerType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("milliseconds", IntegerType(), True),
                                    StructField("fastestLap", IntegerType(), True),
                                    StructField("rank", IntegerType(), True),
                                    StructField("fastestLapTime", StringType(), True),
                                    StructField("fastestLapSpeed", StringType(), True),
                                    StructField("statusId", IntegerType(), True)


])

# COMMAND ----------

results_df = spark.read.schema(results_schema).json("/mnt/f1de/raw/results.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Rename columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import col, concat, current_timestamp, lit

# COMMAND ----------

results_withcolumns_df = results_df.withColumnRenamed("resultId", "result_id")\
                                   .withColumnRenamed("raceId", "race_id")\
                                   .withColumnRenamed("driverId", "driver_id")\
                                   .withColumnRenamed("constructorId", "constructor_id")\
                                   .withColumnRenamed("positionText", "position_text")\
                                   .withColumnRenamed("positionOrder", "position_order")\
                                   .withColumnRenamed("fastestLap", "fastest_lap")\
                                   .withColumnRenamed("fastestLapTime", "fastest_lap_time")\
                                   .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed")\
                                   .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Drop the unwanted columns
# MAGIC
# MAGIC

# COMMAND ----------

results_df_final = results_withcolumns_df.drop(results_withcolumns_df['statusId'])

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Step 4 - Write to output to processed container in parquet format
# MAGIC

# COMMAND ----------

#results_df_final.write.mode("overwrite").partitionBy("race_id").parquet("/mnt/f1de/processed/results")

results_df_final.write.mode("overwrite").partitionBy("race_id").format("parquet").saveAsTable("f1_processed.results")