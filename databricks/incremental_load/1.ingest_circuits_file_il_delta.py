# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Ingest circuits.csv file

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Step 1 - Read the CSV file using the spark dataframe reader

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs 
# MAGIC
# MAGIC ls /mnt/f1de/raw/

# COMMAND ----------

circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                     StructField("circuitRef", StringType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("location", StringType(), True),
                                     StructField("country", StringType(), True),
                                     StructField("lat", DoubleType(), True),
                                     StructField("lng", DoubleType(), True),
                                     StructField("alt", IntegerType(), True),
                                     StructField("url", StringType(), True),
])

# COMMAND ----------

circuits_df = spark.read.option("header", True).schema(circuits_schema).csv(f"{raw_incremental_load_folder_path}/{v_file_date}/circuits.csv")

# COMMAND ----------

# creating df for circuits
"""
by using inferSchema we are increasing the numbers of Spark Jobs. In a production level, or working with a huge amount of data
that is not sustenible. In order to workaround, it's better to 


circuits_df = spark.read.option("header", True).option("inferSchema", True).csv("dbfs:/mnt/f1de/raw/circuits.csv")

"""

# COMMAND ----------

# checking Schema

circuits_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### select only the columns wanted

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %md
# MAGIC #### step 2 - selecting only the columns wanted
# MAGIC

# COMMAND ----------

circuits_selected_df= circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))


# COMMAND ----------

# MAGIC %md
# MAGIC #### step 3 - rename columns

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id") \
    .withColumnRenamed("circuitRef", "circuit_ref") \
    .withColumnRenamed("lat", "latitude") \
    .withColumnRenamed("lng", "longitude") \
    .withColumnRenamed("alt", "altitude") \
    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC #### step 4 - Add ingestion date to the dataframe

# COMMAND ----------

circuits_final_df = add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step - 5 Writing the file into the data lake as Parquet file

# COMMAND ----------

#circuits_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}circuits")

# COMMAND ----------

circuits_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.circuits")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from f1_processed.circuits;

# COMMAND ----------

dbutils.notebook.exit("Success")