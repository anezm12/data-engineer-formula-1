# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Ingest drivers.json file

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
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

# Define the inside schema for column "name"

name_schema = StructType(fields=[StructField("forename", StringType(), True),
                                 StructField("surname", StringType(), True)
])

# COMMAND ----------

# define the outside schema driver schema

driver_schema = StructType(fields=[StructField("driverId", IntegerType(), False),
                                   StructField("driverRef", StringType(), True),
                                   StructField("number", IntegerType(), True),
                                   StructField("code", StringType(), True),
                                   StructField("name", name_schema),
                                   StructField("dob", DateType(), True),
                                   StructField("nationality", StringType(), True),
                                   StructField("url", StringType(), True)

])

# COMMAND ----------

drivers_df = spark.read.schema(driver_schema).json(f"{raw_incremental_load_folder_path}/{v_file_date}/drivers.json")

# COMMAND ----------

drivers_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Rename columns and add new columns
# MAGIC
# MAGIC 1. driverId renamed to driver_id
# MAGIC 2. driverRef renamed to driver_ref
# MAGIC 3. Ingestion date added
# MAGIC 4. Name added with concatenation of forename and surname

# COMMAND ----------

from pyspark.sql.functions import col, concat, current_timestamp, lit

# COMMAND ----------

driver_with_columns_df = drivers_df.withColumnRenamed("driverId", "driver_id") \
                                    .withColumnRenamed("driverRef", "driver_ref") \
                                    .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname"))) \
                                    .withColumn("data_source", lit(v_data_source)) \
                                    .withColumn("file_date", lit(v_file_date))

# driver_with_columns_df = drivers_df.withColumnRenamed("driverId", "driver_id")\
#                                    .withColumnRenamed("driverRef", "driver_ref")\
#                                    .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname")))

# COMMAND ----------

driver_with_columns1_df = add_ingestion_date(driver_with_columns_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Drop the unwanted columns
# MAGIC
# MAGIC 1. name.fornename
# MAGIC 2. name.surname
# MAGIC 3. url

# COMMAND ----------

drivers_final_df = driver_with_columns1_df.drop(driver_with_columns_df['url'])

# COMMAND ----------

#drivers_final1_df = drivers_final_df.withColumn("data_source", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Step 4 - Write to output to processed container in parquet format
# MAGIC

# COMMAND ----------

#drivers_final_df.write.mode("overwrite").parquet("/mnt/f1de/processed/drivers")

drivers_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.drivers")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from f1_processed.drivers
# MAGIC
# MAGIC