# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Read all the data as required

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

driver_df = spark.read.format("delta").load(f"{processed_folder_path}/drivers")\
    .withColumnRenamed("number", "driver_number")\
    .withColumnRenamed("name", "driver_name")\
    .withColumnRenamed("nationality", "driver_nationality")

# COMMAND ----------

constructors_df = spark.read.format("delta").load(f"{processed_folder_path}constructors")\
    .withColumnRenamed("name", "team")

# COMMAND ----------

circuits_df = spark.read.format("delta").load(f"{processed_folder_path}circuits")\
    .withColumnRenamed("location", "circuit_location")

# COMMAND ----------

races_df = spark.read.format("delta").load(f"{processed_folder_path}races")\
    .withColumnRenamed("name", "race_name")\
    .withColumnRenamed("race_timestamp", "race_date")

# COMMAND ----------

# here we are going to apply a filter base on the date the file was ingested.
results_df = spark.read.format("delta").load(f"{processed_folder_path}results")\
    .filter(f"data_source = '{v_file_date}'") \
    .withColumnRenamed("time", "race_time") \
    .withColumnRenamed("race_id", "results_race_id") \
    .withColumnRenamed("data_source", "results_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Joinscircuits to races

# COMMAND ----------

# joining race and circuits

race_circuits_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, "inner")\
    .select(races_df.race_id, races_df.race_year, races_df.race_name, races_df.race_date, circuits_df.circuit_location)

# COMMAND ----------

# joining all the tables

race_results_df = results_df.join(race_circuits_df, results_df.results_race_id == race_circuits_df.race_id)\
                            .join(driver_df, results_df.driver_id == driver_df.driver_id)\
                            .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id)

# COMMAND ----------

final_df = race_results_df.select("race_id","race_year", "race_name", "race_date", "circuit_location", "driver_name", "driver_number",
                                   "driver_nationality", "team", "grid", "fastest_lap_time", "race_time", "points", "position", "results_file_date") \
                                   .withColumnRenamed("results_file_date", "file_date")

# COMMAND ----------

merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_id = src.race_id"
merge_delta_data(final_df, 'f1_presentation', 'race_results', presentation_folder_path, merge_condition, 'race_id')