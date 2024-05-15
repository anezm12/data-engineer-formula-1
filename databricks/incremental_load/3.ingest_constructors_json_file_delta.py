# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Ingest constructors.json file
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/configuration"
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the JSON File using the spark dataframe reader

# COMMAND ----------

# This is just another way to define the schema

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructors_df = spark.read.schema(constructors_schema).json(f"{raw_incremental_load_folder_path}/{v_file_date}/constructors.json")

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
from pyspark.sql.functions import lit

# COMMAND ----------

constructor_final_df= constructor_dropped_df.withColumnRenamed("constructorId", "constructor_id")\
                                            .withColumnRenamed("constructorRef", "constructor_ref") \
                                            .withColumn("data_source", lit(v_data_source)) \
                                            .withColumn("file_date", lit(v_file_date))
                                            

# COMMAND ----------

constructor_final1_df = add_ingestion_date(constructor_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 - Write output to parquet file
# MAGIC

# COMMAND ----------

#constructor_final_df.write.mode("overwrite").parquet("/mnt/f1de/processed/constructors")

constructor_final1_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.constructors")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from f1_processed.constructors;