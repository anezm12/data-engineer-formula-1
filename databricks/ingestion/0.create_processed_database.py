# Databricks notebook source
# MAGIC %sql
# MAGIC create database IF not exists f1_processed
# MAGIC location "/mnt/f1de/processed"

# COMMAND ----------

# MAGIC %sql
# MAGIC desc database f1_processed

# COMMAND ----------

# MAGIC %sql
# MAGIC use f1_processed;
# MAGIC select * from qualifying