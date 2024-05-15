# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Ingest drivers.json file

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

drivers_df = spark.read.schema(driver_schema).json("/mnt/f1de/raw/drivers.json")

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

driver_with_columns_df = drivers_df.withColumnRenamed("driverId", "driver_id")\
                                   .withColumnRenamed("driverRef", "driver_ref")\
                                   .withColumn("ingestion_date", current_timestamp())\
                                   .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Drop the unwanted columns
# MAGIC
# MAGIC 1. name.fornename
# MAGIC 2. name.surname
# MAGIC 3. url

# COMMAND ----------

drivers_final_df = driver_with_columns_df.drop(driver_with_columns_df['url'])

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Step 4 - Write to output to processed container in parquet format
# MAGIC

# COMMAND ----------

#drivers_final_df.write.mode("overwrite").parquet("/mnt/f1de/processed/drivers")

drivers_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.drivers")