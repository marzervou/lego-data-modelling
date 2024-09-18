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

# MAGIC %md
# MAGIC The code snippet performs the following operations:
# MAGIC
# MAGIC 1. It filters the DataFrame `df` to exclude rows where the `availability_status` column has the value `"R_RETIRED"`. This is achieved using the `.filter()` method with the condition `df.availability_status != "R_RETIRED"`.
# MAGIC 2. The resulting filtered DataFrame is stored in `df_filtered`.

# COMMAND ----------

df_filtered = df.filter(df.availability_status != "R_RETIRED")
print(f"Original count: {df.count()}, Filtered count: {df_filtered.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC The code snippet creates a new DataFrame called `df_selected_columns` by selecting specific columns from the `df_filtered` DataFrame. The selected columns are "headline_text", "product_name", "product_description", "category_name", and "site_product_identifier". 
# MAGIC
# MAGIC After creating the new DataFrame, it is displayed using the `display()` function.

# COMMAND ----------

df_selected_columns = df_filtered.select("headline_text", "product_name", "product_description", "category_name", "site_product_identifier")
display(df_selected_columns)

# COMMAND ----------

# MAGIC %md
# MAGIC The code snippet creates a new DataFrame called `df_deduped` by performing two operations on the `df_selected_columns` DataFrame. 
# MAGIC
# MAGIC First, the `dropDuplicates()` function is used to remove any duplicate rows based on the "site_product_identifier" column. This ensures that only unique rows remain in the DataFrame.
# MAGIC
# MAGIC Second, the `filter()` function is used to filter out any rows where the "product_description" column is null. This removes any rows that do not have a product description.
# MAGIC
# MAGIC Finally, the resulting DataFrame `df_deduped` is displayed using the `display()` function.

# COMMAND ----------

df_deduped = df_selected_columns.dropDuplicates(["site_product_identifier"]).filter(df_selected_columns.product_description.isNotNull())
display(df_deduped)

# COMMAND ----------

# MAGIC %md
# MAGIC The code snippet creates a new DataFrame called `df_concatenated` by adding a new column called "concatenated_text" to the `df_deduped` DataFrame. 
# MAGIC
# MAGIC The "concatenated_text" column is created by concatenating the values of several columns: "headline_text", "product_name", "product_description", "category_name", and "site_product_identifier". The values in between each column are specified as separators, represented by the string " | ".
# MAGIC
# MAGIC Finally, the resulting DataFrame `df_concatenated` is displayed using the `display()` function.

# COMMAND ----------

from pyspark.sql.functions import concat, col, lit

df_concatenated = df_deduped.withColumn("concatenated_text", concat(col("headline_text"), lit(" | "), col("product_name"), lit(" | "), col("product_description"), lit(" | "), col("category_name"), lit(" | "), col("site_product_identifier")))

display(df_concatenated)

# COMMAND ----------

df_concatenated.write.format("delta").mode("overwrite").saveAsTable("shared.lego.silver_data")

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE `shared`.`lego`.`silver_data` 
# MAGIC SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
