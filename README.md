# Data Engineer Formula 1

![formula1](https://github.com/anezm12/data-engineer-formula-1/assets/101163640/73d33416-5c71-430d-83c8-420f3a596ca1)

## Overview

I created and managed notebooks, dashboards, clusters, and jobs in Azure Databricks, and used PySpark and Spark SQL for data ingestion, transformation, and analysis. Additionally, I explored Data Lake and Lakehouse architectures, implemented Lakehouse architecture with Delta Lake, and created and managed ADF pipelines and triggers to automate and monitor workflows.

## Azure Data Lake

For this project, I chose the storage account containers (data lake gen 2) for the storage of the data in different levels of cure. I followed the medallion architecture, which is a data design pattern used to logically organize data in a lakehouse, with the goal of incrementally and progressively improving the structure and quality of data as it flows through each layer of the architecture. In my case, I decided to name the containers raw, processed, and presentation. The data came as batches and consumed by dates as I will explain in the databricks section.

This image was taken from databricks website refer to them for more details:

https://www.databricks.com/glossary/medallion-architecture

![medallion architecture](https://github.com/anezm12/data-engineer-formula-1/assets/101163640/6204da09-b4f4-4142-a6d9-4ec7253186ec)

Storage Account Project: 

![Storage Account drawio](https://github.com/anezm12/data-engineer-formula-1/assets/101163640/e21671b1-c47b-4427-9f09-6b92fdf1a039)


## Azure DataBricks

Compute Summary

![databricks compute](https://github.com/anezm12/data-engineer-formula-1/assets/101163640/fba87133-78bb-46ab-ae69-c9dbac3d22bb)

You can check all the notebooks in the databricks folder.


<ul>
  <li> <b>Includes</b>, here you can find two notebooks the configuration one has all the paths for the containers used during the project. Common_functions where are the functions used on other notebooks.</li>
  <li><b>set-up</b>, mount containers</li>
  <li><b>incremental_load_delta</b>, I ingested the raw files and set up a process to handle new data arriving every weekend during the F1 season. The raw data is transformed to ensure it is clean and useful for analysis.</li>
  <li><b>trans_inc_load_delta</b>, these notebooks handle the transformations needed to generate the race results, drivers, and constructors' standings. </li>
  <li><b>analysis</b>, Finally, these SQL notebooks analyze the previously created tables to identify the most dominant driver and team. </li>
</ul>

## Azure Data Factory

Ingest Pipeline

![data factory 1](https://github.com/anezm12/data-engineer-formula-1/assets/101163640/53bf2610-371a-4933-9f5c-382eb88edfca)

Transformation Pipeline

![data factory 2](https://github.com/anezm12/data-engineer-formula-1/assets/101163640/c920800a-d502-4883-b250-382c6cfb826c)

Process Pipeline

![data factory 3](https://github.com/anezm12/data-engineer-formula-1/assets/101163640/a1dc032b-86d5-4edc-a6a4-44a08f23163a)

Trigger

![trigger](https://github.com/anezm12/data-engineer-formula-1/assets/101163640/48ccd039-4a02-4206-8578-ae8fd7fe9327)

Results

![pipeline runs](https://github.com/anezm12/data-engineer-formula-1/assets/101163640/fd4ba4f5-5b0b-440a-8802-799c46aba03f)

