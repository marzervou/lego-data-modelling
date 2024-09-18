# Databricks notebook source
# MAGIC %md
# MAGIC These are some of the tags we want to create:
# MAGIC
# MAGIC     - Occasion: (Christmas Gift, Birthday Gift, Holiday Gift, Everyday Play, Special Treat)
# MAGIC     - Age Range: (Preschool, Kids, Teenagers, Adults)
# MAGIC     - Theme: (Adventure, Animals, Cars and Trucks, Fantasy, Space, Superheroes)
# MAGIC     - Skill Level: (Beginner, Intermediate, Advanced)
# MAGIC     - Play Style:(Building and Construction, Imaginative Play, Action and Adventure, Strategy and Puzzle-Solving)
# MAGIC     - Franchise:(Star Wars, Harry Potter, Marvel, DC Comics, Disney, Pixar)
# MAGIC     - Product Type:(Building Sets, Vehicles, Minifigures, Accessories, Books and Comics)

# COMMAND ----------

df_concatenated = spark.read.format("delta").table("shared.lego.silver_data")
display(df_concatenated)

# COMMAND ----------

from pyspark.sql.functions import col, lower, regexp_extract

# Lowercase the concatenated_text column
df_lowercased = df_concatenated.withColumn("concatenated_text_lower", lower(col("concatenated_text")))

# Apply regex to identify if the product is a Christmas present or not
df_tagged = df_lowercased.withColumn("is_christmas_present", regexp_extract(col("concatenated_text_lower"), r"\b(christmas|xmas)\b", 0))


display(df_tagged)
