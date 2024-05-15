# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Ingest constructors.json file
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the JSON File using the spark dataframe reader

# COMMAND ----------

# This is just another way to define the schema

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructors_df = spark.read.schema(constructors_schema).json("/mnt/f1de/raw/constructors.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2- Drop unwanted column

# COMMAND ----------

constructor_dropped_df = constructors_df.drop(constructors_df['url'])

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Step 3 - Rename columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

constructor_final_df= constructor_dropped_df.withColumnRenamed("constructorId", "constructor_id")\
                                            .withColumnRenamed("constructorRef", "constructor_ref")\
                                            .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 - Write output to parquet file
# MAGIC

# COMMAND ----------

#constructor_final_df.write.mode("overwrite").parquet("/mnt/f1de/processed/constructors")

constructor_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.constructors")