# Data Engineer Formula 1

![formula1](https://github.com/anezm12/data-engineer-formula-1/assets/101163640/73d33416-5c71-430d-83c8-420f3a596ca1)


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
  <li><b></b></li>
</ul>
