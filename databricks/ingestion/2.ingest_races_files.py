# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Ingest races.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Read the CSV file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                  StructField("year", IntegerType(), True),
                                  StructField("round", IntegerType(), True),
                                  StructField("circuitId", IntegerType(), True),
                                  StructField("name", StringType(), True),
                                  StructField("date", DateType(), True),
                                  StructField("time", StringType(), True),
                                  StructField("url", StringType(), True)

])

# COMMAND ----------

races_df = spark.read.option("header", True).schema(races_schema).csv("/mnt/f1de/raw/races.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Add ingestion date race_timestamp to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, concat, col, lit


# COMMAND ----------

races_timestamp_df = races_df.withColumn("ingestion_date", current_timestamp())\
                             .withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'))


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Step 3 - Selecting the columns required & rename

# COMMAND ----------

races_select_df = races_timestamp_df.select(col('raceId').alias('race_id'), col('year').alias('race_year'),col('round'),col('circuitId').alias('circuit_id'),col('name'),col('ingestion_date'), col('race_timestamp'))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 - Write the output to processed container in parquet format

# COMMAND ----------

#races_select_df.write.mode('overwrite').partitionBy('race_year').parquet('/mnt/f1de/processed/races')

races_select_df.write.mode("overwrite").partitionBy('race_year').format("parquet").saveAsTable("f1_processed.races")