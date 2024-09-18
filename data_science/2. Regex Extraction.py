# Databricks notebook source
df_concatenated = spark.read.format("delta").table("shared.lego.silver_data")
display(df_concatenated)

# COMMAND ----------

from pyspark.sql.functions import col, lower, regexp_extract

# Lowercase the concatenated_text column
df_lowercased = df_concatenated.withColumn("concatenated_text_lower", lower(col("concatenated_text")))

# Apply regex to identify if the product is a Christmas present or not
df_tagged = df_lowercased.withColumn("is_christmas_present", regexp_extract(col("concatenated_text_lower"), r"\b(christmas|xmas)\b", 0))


display(df_tagged)
