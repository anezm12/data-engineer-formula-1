# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest Qualifying files

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read files

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType

# COMMAND ----------

qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                       StructField("raceId", IntegerType(), False),
                                       StructField("driverId", IntegerType(), False),
                                       StructField("constructorId", IntegerType(), False),
                                       StructField("number", IntegerType(), False),
                                       StructField("position", IntegerType(), True),
                                       StructField("q1", StringType(), True),
                                       StructField("q2", StringType(), True),
                                       StructField("q3", StringType(), True)                                       
])

# COMMAND ----------

qualifying_df = spark.read.schema(qualifying_schema).option("multiLine", True).json(f"{raw_incremental_load_folder_path}/{v_file_date}/qualifying")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 2 - Rename columns and add new columns
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import current_timestamp


# COMMAND ----------

final_df = qualifying_df.withColumnRenamed("qualifyId", "qualify_id")\
                        .withColumnRenamed("raceId", "race_id")\
                        .withColumnRenamed("driverId", "driver_id")\
                        .withColumnRenamed("constructorId", "constructor_id")

# COMMAND ----------

final1_df = add_ingestion_date(final_df)

# COMMAND ----------

display(final1_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Write final version

# COMMAND ----------

merge_condition = "tgt.qualify_id = src.qualify_id AND tgt.race_id = src.race_id"
merge_delta_data(final1_df, 'f1_processed', 'qualifying', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(1) from f1_processed.qualifying