# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Ingest Lap Times folder
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the CSV file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

lap_times_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), False),
                                      StructField("lap", IntegerType(), False),
                                      StructField("position", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True),


])

# COMMAND ----------

"""
since this is a multifiles case you have two options,
one specify the whole folder 

"/mnt/f1de/raw/lap_times/"

or use a wildcard to point to only those files with that pattern 

"/mnt/f1de/raw/lap_times/lap_times_split*.csv"
"""
lap_times_df = spark.read.schema(lap_times_schema).csv("/mnt/f1de/raw/lap_times")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename columns and add new columns
# MAGIC
# MAGIC 1. Rename driverId and recaId
# MAGIC 2. Add ingestion_date with current timestamp

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = lap_times_df.withColumnRenamed("raceId", "race_id").withColumnRenamed("driverId", "driver_id").withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Write to output to processed container in parquet format

# COMMAND ----------

#final_df.write.mode("overwrite").parquet("/mnt/f1de/processed/lap_times")

final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.lap_times")