# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Ingest results.json single line file
# MAGIC

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

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

results_df = spark.read \
    .schema(results_schema) \
    .json(f"{raw_incremental_load_folder_path}/{v_file_date}/results.json")

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
                                   .withColumn("data_source", lit(v_file_date)) 

# COMMAND ----------

results_withcolumns1_df = add_ingestion_date(results_withcolumns_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Drop the unwanted columns
# MAGIC
# MAGIC

# COMMAND ----------

results_df_final = results_withcolumns1_df.drop(results_withcolumns_df['statusId'])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 - Dropping duplicates from the df

# COMMAND ----------

results_deduped_df = results_df_final.dropDuplicates(['race_id', 'driver_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Step 5 - Write to output to processed container in delta format
# MAGIC

# COMMAND ----------

spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning", "true")
from delta.tables import DeltaTable
if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
    deltaTable = DeltaTable.forPath(spark, "/mnt/f1de/processed/results")
    deltaTable.alias("tgt").merge(results_deduped_df.alias("src"),"tgt.result_id = src.result_id AND tgt.race_id = src.race_id").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
else:
    results_deduped_df.write.mode("overwrite").partitionBy("race_id").format("delta").saveAsTable("f1_processed.results")