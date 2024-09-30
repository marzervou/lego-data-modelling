# Databricks notebook source
# MAGIC %run ./config

# COMMAND ----------

catalog = config['catalog']
dbName = config['dbName']
silver_data= config['silver_data']
gold_data= config['gold_data']

# COMMAND ----------

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

# MAGIC %md #Loading ingested data for tagging
# MAGIC This is a companion notebook to load the `lego-product` data as a spark df and save it as a dataframe. 

# COMMAND ----------

df_concatenated = spark.read.format("delta").table(f"{catalog}.{dbName}.{silver_data}")
display(df_concatenated)

# COMMAND ----------

from pyspark.sql.functions import col, lower, regexp_extract

# Lowercase the concatenated_text column
df_lowercased = df_concatenated.withColumn("concatenated_text_lower", lower(col("concatenated_text")))

# Apply regex to identify if the product is a Christmas present or not
df_tagged = df_lowercased.withColumn("is_christmas_present", regexp_extract(col("concatenated_text_lower"), r"\b(christmas|xmas)\b", 0))


display(df_tagged)

# COMMAND ----------

# MAGIC %md #Data Processing, Data cleaning and Enforcing Data Quality
# MAGIC
# MAGIC We can apply some Data quality operations to ensure the integrity and usefulness of the data. These operations can help in cleaning the data, handling missing values, and ensuring that the data is in a suitable format for further analysis or processing.
# MAGIC
# MAGIC Below, we are performing very basic Data cleaning operations such as removing punctuations ( periods (.), commas (,), question marks (?), exclamation points (!) ) and special characters (@, #, $, %, &, *, (, ), _, +, etc.)
# MAGIC
# MAGIC This is a companion notebook to load the `lego-product` data as a spark df and then df_concatenated dataframe will be used for analysis 

# COMMAND ----------

from pyspark.sql.functions import col, lower, regexp_extract
from pyspark.sql.functions import regexp_replace, col

# Lowercase the concatenated_text column
df_lowercased = df_concatenated.withColumn("concatenated_text_lower", lower(col("concatenated_text")))

# Removing punctuation
df_remove_punc = df_lowercased.withColumn("cleaned_text", regexp_replace(col("concatenated_text_lower"), "[.,!?]", ""))

# Removing special characters (broadly, including punctuation)
df_cleaned = df_remove_punc.withColumn("new_concatenated_text_lower", regexp_replace(col("cleaned_text"), "[^a-zA-Z0-9\\s]", ""))

display(df_cleaned)

# COMMAND ----------

# MAGIC %md #Tag the products based on the sub category level 

# COMMAND ----------

occasion_pattern = r"\b(christmas|birthday|holiday|everyday|special|treat)\b"

df_tagged_occasions = df_cleaned.withColumn("occasion_type", regexp_extract(col("new_concatenated_text_lower"), occasion_pattern, 0))

display(df_tagged_occasions)

# COMMAND ----------

# MAGIC %md #Tag the products based on the sub category level for all patterns

# COMMAND ----------

from pyspark.sql.functions import col, lower, regexp_extract


# Define the regex patterns
occasion_pattern = r"\b(christmas|birthday|holiday|everyday|special treat)\b"
age_range_pattern = r"\b(preschool|kids|teenagers|adults)\b"
theme_pattern = r"\b(adventure|animals|cars|trucks|fantasy|space|superheroes)\b"
skill_level_pattern = r"\b(beginner|intermediate|advanced)\b"
play_style_pattern = r"\b(building|construction|imaginative|action|adventure|strategy|puzzle)\b"
franchise_pattern = r"\b(star wars|harry potter|marvel|dc|comics|disney|pixar)\b"
product_type_pattern = r"\b(building sets|vehicles|minifigures|accessories|books|Comics)\b"

df_tagged = df_cleaned.withColumn("occasion_type", regexp_extract(col("new_concatenated_text_lower"), occasion_pattern, 0)) \
                    .withColumn("age_range", regexp_extract(col("new_concatenated_text_lower"), age_range_pattern, 0)) \
                    .withColumn("theme_type", regexp_extract(col("new_concatenated_text_lower"), theme_pattern, 0)) \
                    .withColumn("skill_level", regexp_extract(col("new_concatenated_text_lower"), skill_level_pattern, 0)) \
                    .withColumn("play_style", regexp_extract(col("new_concatenated_text_lower"), play_style_pattern, 0)) \
                    .withColumn("franchise_type", regexp_extract(col("new_concatenated_text_lower"), franchise_pattern, 0)) \
                    .withColumn("product_type", regexp_extract(col("new_concatenated_text_lower"), product_type_pattern, 0))


display(df_tagged)

# COMMAND ----------

# MAGIC %md #Filter the products based on the tags

# COMMAND ----------

filtered_df = df_tagged.filter(
    (col("occasion_type") == "christmas") | 
    (col("age_range") == "kids")
)

display(filtered_df)

# COMMAND ----------

# MAGIC %md #Tags based on the category (However, it needs some extra operations to highlight the correct tags)

# COMMAND ----------

from pyspark.sql.functions import col, lower, regexp_extract, when, lit



# Define regex patterns for each category
patterns = {
    'occasion': r'(christmas|birthday|holiday|everyday|special|treat)',
    'age_range': r'(preschool|kids|teenagers|adults)',
    'theme': r'(adventure|animals|cars and trucks|fantasy|space|superheroes)',
    'skill_level': r'(beginner|intermediate|advanced)',
    'play_style': r'(building|construction|imaginative|play|action|adventure|strategy|puzzle)',
    'franchise': r'(star wars|harry|potter|marvel|dc|comics|disney|pixar)',
    'product_type': r'(building|sets|vehicles|minifigures|accessories|books|comics)'
}

# Initialize the "category" column
df_tagged = df_cleaned.withColumn("category", lit("Unknown"))

# Sequentially check each pattern and update the "category" column with the matched pattern
for category, pattern in patterns.items():
    df_tagged = df_tagged.withColumn("category", 
                                     when(regexp_extract(col("concatenated_text_lower"), pattern, 0) != "", regexp_extract(col("concatenated_text_lower"), pattern, 0))
                                     .otherwise(col("category")))

display(df_tagged)

# COMMAND ----------

# MAGIC %md #You can use SQL code as well to process and perform analysis on the Data within same notebook

# COMMAND ----------

# Create a temporary view from your DataFrame
df_cleaned.createOrReplaceTempView("concatenated_view")

# SQL query to add tags based on regex patterns with lowercase keywords
sql_query = """
SELECT *,
       REGEXP_EXTRACT(new_concatenated_text_lower, '(christmas|birthday|holiday|everyday|special|treat|gift)', 0) AS occasion_type,
       REGEXP_EXTRACT(new_concatenated_text_lower, '(preschool|kids|teenagers|adults)', 0) AS age_range,
       REGEXP_EXTRACT(new_concatenated_text_lower, '(adventure|animals|cars|trucks|fantasy|space|superheroes)', 0) AS theme_type,
       REGEXP_EXTRACT(new_concatenated_text_lower, '(beginner|intermediate|advanced)', 0) AS skill_level,
       REGEXP_EXTRACT(new_concatenated_text_lower, '(building|construction|imaginative|action|adventure|strategy|puzzle)', 0) AS play_type,
       REGEXP_EXTRACT(new_concatenated_text_lower, '(star wars|harry potter|marvel|dc comics|disney|pixar)', 0) AS franchise_type,
       REGEXP_EXTRACT(new_concatenated_text_lower, '(building|sets|vehicles|minifigures|accessories|books|comics)', 0) AS product_type
FROM concatenated_view
"""

# Execute the SQL query and display the result
df_tagged_sql = spark.sql(sql_query)
display(df_tagged_sql)

# COMMAND ----------

df_tagged_occasions.createOrReplaceTempView("tagged_occasions_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC Select concatenated_text_lower from tagged_occasions_view where concatenated_text_lower ILIKE '%adults%'

# COMMAND ----------

from pyspark.sql.functions import col, expr

# Simplify flag creation with expr for conditional logic
flag_conditions = [
    ("play_type = 'action'", "action_flag"),
    ("franchise_type = 'star wars'", "star_wars_flag"),
    ("occasion_type = 'special'", "special_flag"),
    ("product_type = 'minifigures'", "minifigures_flag"),
    ("(play_type = 'building' OR theme_type = 'building')", "building_flag"),
    ("product_type = 'accessories'", "accessories_flag"),
    ("product_type = 'sets'", "sets_flag"),
    ("theme_type = 'animals'", "animals_flag"),
    ("occasion_type = 'holiday'", "holiday_flag"),
    ("age_range = 'kids'", "kids_flag"),
    ("occasion_type = 'gift'", "gift_flag"),
    ("skill_level = 'advanced'", "advanced_flag"),
    ("theme_type = 'adventure'", "adventure_flag"),
    ("franchise_type = 'disney'", "disney_flag"),
    ("franchise_type = 'marvel'", "marvel_flag"),
    ("theme_type = 'trucks'", "trucks_flag"),
    ("product_type = 'vehicles'", "vehicles_flag"),
    ("product_type = 'comics'", "comics_flag"),
    ("play_type = 'strategy'", "strategy_flag"),
    ("play_type = 'construction'", "construction_flag"),
    ("theme_type = 'space'", "space_flag"),
    ("occasion_type = 'christmas'", "christmas_flag"),
    ("occasion_type = 'everyday'", "everyday_flag"),
    ("age_range = 'adults'", "adults_flag"),
    ("occasion_type = 'birthday'", "birthday_flag"),
    ("occasion_type = 'treat'", "treat_flag"),
    ("franchise_type = 'harry potter'", "harry_potter_flag"),
    ("theme_type = 'cars'", "cars_flag")
]

for condition, flag_name in flag_conditions:
    df_tagged_sql = df_tagged_sql.withColumn(flag_name, expr(f"CASE WHEN {condition} THEN 1 ELSE 0 END"))

# COMMAND ----------

display(df_tagged_sql)

# COMMAND ----------

df_tagged_sql.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{catalog}.{dbName}.{gold_data}_regex")

# COMMAND ----------

# MAGIC %md
# MAGIC #Write unit tests for the PySpark solution provided in your notebook, you can use the pyspark.sql.functions module along with a testing framework like pytest
# MAGIC
# MAGIC The idea is to create a small, mock DataFrame that simulates the structure of your actual data, apply your function, and then assert the outcomes against expected results.
# MAGIC

# COMMAND ----------

# MAGIC %pip install pytest

# COMMAND ----------

def test_tag_text_with_category(spark):
    # Define a schema for the mock DataFrame
    schema = StructType([
        StructField("new_concatenated_text_lower", StringType(), True)
    ])
    
    # Create a mock DataFrame
    data = [("Christmas is coming",), ("Advanced robotics",)]
    df = spark.createDataFrame(data, schema=schema)
    
    # Define expected data
    expected_data = [("Christmas is coming", "christmas"), ("Advanced robotics", "advanced")]
    expected_schema = StructType([
        StructField("concatenated_text", StringType(), True),
        StructField("category", StringType(), True)
    ])
    expected_df = spark.createDataFrame(expected_data, schema=expected_schema)
    
    # Define regex patterns for each category
    patterns = {
        'occasion': r'(christmas|birthday|holiday|everyday|special|treat)',
        'skill_level': r'(beginner|intermediate|advanced)'
    }
    
    # Apply the function
    tagged_df = tag_text_with_category(df, patterns)
    
    # Collect and compare the results
    assert tagged_df.collect() == expected_df.collect(), "Test Failed!"
    print("Test Passed!")

# Define the tag_text_with_category function
def tag_text_with_category(df, patterns):
    from pyspark.sql.functions import regexp_extract
    
    for category, pattern in patterns.items():
        df = df.withColumn(category, regexp_extract(df.new_concatenated_text_lower, pattern, 1))
    
    return df

# COMMAND ----------

from pyspark.sql.functions import when, col, count
from pyspark.sql import Row
# Only as a fallback, not recommended for your scenario as per the instructions
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# Correct the column names according to your DataFrame's actual column names
category_columns = ['theme', 'age_range', 'occasion','skill_level','play_style','franchise','product_type'] 

# Counting tagged vs. untagged for each category
tagged_counts = []
for category in category_columns:
    tagged_count = df_tagged.select(
        count(when(col(category) != '', True)).alias(f'{category}_tagged'),
        count(when(col(category) == '', True)).alias(f'{category}_untagged')
    ).collect()[0]
    tagged_counts.append(tagged_count)

# Convert to DataFrame for visualization
tagged_df_rows = [Row(category=category, tagged=tc[0], untagged=tc[1]) for category, tc in zip(category_columns, tagged_counts)]
tagged_df = spark.createDataFrame(tagged_df_rows)

# Show the DataFrame
display(tagged_df)
