# Databricks notebook source
# MAGIC %md
# MAGIC The ingestion code snippet performs the following operations:
# MAGIC
# MAGIC 1. It uses the Spark DataFrame API to read data from a CSV file located at `/Volumes/shared/lego/raw_data/product_catalogue_categories.csv`.
# MAGIC 2. The `.format("csv")` method specifies that the input file format is CSV.
# MAGIC 3. The `.option("header", "true")` method indicates that the first row of the CSV file contains column names, which should be used as DataFrame column headers.
# MAGIC 4. The `.load()` method loads the data from the specified path into a Spark DataFrame named `df`.
# MAGIC 5. Finally, `display(df)` is used to visually display the DataFrame within the Databricks notebook, allowing for a rich representation of the data structure and its contents.

# COMMAND ----------

df = spark.read.format("csv").option("header", "true").load("/Volumes/shared/lego/raw_data/product_catalogue_categories.csv")
display(df)

# COMMAND ----------


