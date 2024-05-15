# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest Qualifying files

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

qualifying_df = spark.read.schema(qualifying_schema).option("multiLine", True).json("/mnt/f1de/raw/qualifying")

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
                        .withColumnRenamed("constructorId", "constructor_id")\
                        .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Write final version

# COMMAND ----------

#final_df.write.mode("overwrite").parquet("/mnt/f1de/processed/qualifying")

final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.qualifying")