# Databricks notebook source
# MAGIC %run /Workspace/Users/maria.zervou@databricks.com/lego-data-modelling/data_science/config.py

# COMMAND ----------

catalog = config['catalog']
db = config['dbName']
llm = config['llm_model_serving_endpoint_name']
WRITE_OT_NAME = config['gold_data_llm']

# COMMAND ----------

# For this first basic demo, we'll keep the configuration as a minimum. In real app, you can make all your RAG as a param (such as your prompt template to easily test different prompts!)
chain_config = {
    "llm_model_serving_endpoint_name": llm,  # the foundation model we want to use
    "llm_prompt_template": """You are an assistant that tags Lego products. Given the product description {PRODUCT_DESCRIPTION} tag the description against all of the following categories:
    - Occasion: (Christmas Gift, Birthday Gift, Holiday Gift, Everyday Play, Special Treat)
    - Age Range: (Preschool, Kids, Teenagers, Adults)
    - Theme: (Adventure, Animals, Cars and Trucks, Fantasy, Space, Superheroes)
    - Skill Level: (Beginner, Intermediate, Advanced)
    - Play Style:(Building and Construction, Imaginative Play, Action and Adventure, Strategy and Puzzle-Solving)
    - Franchise:(Star Wars, Harry Potter, Marvel, DC Comics, Disney, Pixar)
    - Product Type:(Building Sets, Vehicles, Minifigures, Accessories, Books and Comics)
      Return the result in a JSON format ONLY do not add more text do not add any string characters but the json object. If it unclear or not explicitly mentioned in the description return []
     """
}

# COMMAND ----------

# MAGIC %md
# MAGIC ### Documentation for LangChain Core and Community Modules
# MAGIC
# MAGIC This section outlines the usage of key components from the LangChain Core and Community libraries to facilitate chat-based interactions with language models.
# MAGIC
# MAGIC - **ChatPromptTemplate**: A class from `langchain_core.prompts` used to create and manage prompt templates for chat-based interactions. It allows for dynamic insertion of system and user messages into the prompt sent to the language model.
# MAGIC
# MAGIC - **ChatDatabricks**: A class from `langchain_community.chat_models` designed to interface with chat models hosted on Databricks. It configures the endpoint and additional parameters (e.g., temperature, max tokens) for generating responses from the model.
# MAGIC
# MAGIC - **itemgetter**: A utility from the `operator` module that facilitates efficient item retrieval by index from a list or keys from a dictionary. Useful for data manipulation and extraction tasks.
# MAGIC
# MAGIC - **StrOutputParser**: A class from `langchain_core.output_parsers` that parses the string output from language models into structured data. It is particularly useful for processing and interpreting the responses from chat models.
# MAGIC
# MAGIC The combination of these components enables the construction of sophisticated chat-based applications with language models, allowing for dynamic interaction patterns and efficient data handling.
# MAGIC prompt = ChatPromptTemplate.from_messages(
# MAGIC     [  
# MAGIC         ("system", chain_config.get("llm_prompt_template")), # Contains the instructions from the configuration
# MAGIC         ("user", "{question}") #user's questions
# MAGIC     ]
# MAGIC )
# MAGIC
# MAGIC

# COMMAND ----------

from langchain_core.prompts import ChatPromptTemplate
from langchain_community.chat_models import ChatDatabricks
from operator import itemgetter
from langchain_core.output_parsers import StrOutputParser

prompt = ChatPromptTemplate.from_messages(
    [  
        ("system", chain_config.get("llm_prompt_template")), # Contains the instructions from the configuration
        ("user", "{question}") #user's questions
    ]
)

# Our foundation model answering the final prompt
model = ChatDatabricks(
    endpoint=chain_config.get("llm_model_serving_endpoint_name"),
    extra_params={"temperature": 0.01, "max_tokens": 500}
)

#Let's try our prompt:
answer = (prompt | model | StrOutputParser()).invoke({'question':'Tag the following product description:', 'PRODUCT_DESCRIPTION': """Cuddle up to an everyday hero with THE LEGO® MOVIE 2™ 853879 Emmet Plush. This 12” (32cm) tall stuffed toy is an upscaled version of the Emmet minifigure and features brushed tricot fabric with silkscreen-printed decoration and embroidered face details"""})


# COMMAND ----------

answer

# COMMAND ----------

# MAGIC %md
# MAGIC ### Applying Model to Product Descriptions
# MAGIC
# MAGIC This section of the code demonstrates how to apply a pre-defined model to tag product descriptions within a Spark DataFrame. The process involves reading a Delta table, defining a User-Defined Function (UDF) to apply the model, and then applying this UDF to each product description in the DataFrame.
# MAGIC
# MAGIC #### Steps:
# MAGIC
# MAGIC 1. **Read Delta Table:**
# MAGIC    - The `spark.read.format("delta").table("shared.lego.gold_data").repartition(36)` command reads the Delta table located at `"shared.lego.gold_data"` into a DataFrame, `df_concatenated`, and repartitions it into 36 partitions for optimization.
# MAGIC
# MAGIC 2. **Define UDF:**
# MAGIC    - A UDF, `apply_model`, is defined to apply the tagging model to each product description. It takes a single argument, `description`, which is the product description text.
# MAGIC    - The model is applied via the `prompt | model | StrOutputParser()` pipeline, which constructs a prompt, applies the model, and parses the string output.
# MAGIC    - The UDF is registered with Spark SQL, specifying its return type as `StringType()`.
# MAGIC
# MAGIC 3. **Apply UDF:**
# MAGIC    - The UDF `apply_model_udf` is applied to each row of the DataFrame `df_concatenated` by using the `.withColumn()` method. It creates a new column, `tagged_description`, which contains the model's output for each product description.
# MAGIC    - The source column for product descriptions is assumed to be `"concatenated_text"` in `df_concatenated`.
# MAGIC
# MAGIC 4. **Display Results:**
# MAGIC    - Finally, `display(df_gold)` is used to visualize the DataFrame with the newly added `tagged_description` column, showcasing the tagged product descriptions.
# MAGIC
# MAGIC This process enables the tagging of product descriptions at scale using Spark DataFrames and a custom model applied through a UDF.

# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

df_concatenated = spark.read.format("delta").table("shared.lego.gold_data").repartition(36)

# Define a UDF that applies the model to each product description
def apply_model(description):
    answer = (prompt | model | StrOutputParser()).invoke({'question':'Tag the following product description:', 'PRODUCT_DESCRIPTION': description})
    return answer

# Register the UDF
apply_model_udf = udf(apply_model, StringType())

# Assuming df is your DataFrame and it has a column named 'description' with product descriptions
df_gold = df_concatenated.withColumn("tagged_description", apply_model_udf(df_concatenated["concatenated_text"]))

display(df_gold)


# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import json

# Define a UDF to clean the output and return only the JSON object
@udf(returnType=StringType())
def clean_output(output):
    try:
        # Remove unnecessary characters and return only the JSON object
        cleaned_output = output.replace('```', '').replace('[{', '{').replace('}]', '}').strip()
        # Ensure it's a valid JSON string
        json_obj = json.loads(cleaned_output)
        return json.dumps(json_obj)
    except Exception as e:
        # In case of error, return the original output for troubleshooting
        return output

# Apply the cleaning UDF to the 'tagged_description' column
df_cleaned = df_gold.withColumn("cleaned_tagged_description", clean_output(df_gold["tagged_description"]))
display(df_cleaned)

# COMMAND ----------

df_cleaned_limit = df_cleaned.limit(20)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Processing JSON Data in Spark DataFrame
# MAGIC
# MAGIC This portion of the code processes JSON data in a Spark DataFrame. Here are the steps involved:
# MAGIC
# MAGIC 1. Parse the `cleaned_tagged_description` column from a JSON string into a MapType using the `from_json` function and a specified JSON schema.
# MAGIC 2. Explode the map into key-value rows using the `explode` function, resulting in a DataFrame with columns `Key` and `Values`.
# MAGIC 3. Explode the values from each key into separate rows using the `explode` function again, resulting in a DataFrame with columns `Key` and `Value`.
# MAGIC 4. Get all distinct values across the exploded `Value` column using the `distinct` function.
# MAGIC 5. Collect the distinct values into a Python list, excluding any `None` values.
# MAGIC 6. Iterate over each distinct value and create a new column in the `df_parsed` DataFrame using the `withColumn` function. The column name is dynamically generated based on the distinct value, with spaces replaced by underscores.
# MAGIC 7. The new column is created by checking if the distinct value is present in either the `Occasion` or `Age Range` arrays within the `cleaned_tagged_description` column. The result is cast to a boolean and then to an integer.
# MAGIC 8. Finally, the resulting DataFrame with the new columns is displayed using the `display` function.
# MAGIC
# MAGIC This code snippet performs data processing and transformation to add flags based on distinct values in the JSON data.

# COMMAND ----------

from pyspark.sql.functions import col, array_contains, explode, from_json, map_keys
from pyspark.sql.types import MapType, StringType, ArrayType
# Assuming cleaned_tagged_description is a JSON string, first parse it into a MapType
json_schema = MapType(StringType(), ArrayType(StringType()))

# Step 2: Parse the JSON string column into a map of key -> array of strings
df_parsed = df_cleaned_limit.withColumn("cleaned_tagged_description", from_json(col("cleaned_tagged_description"), json_schema))

# Step 3: Explode the map into key-value rows to get all distinct values
df_exploded = df_parsed.select(explode(col("cleaned_tagged_description")).alias("Key", "Values"))

# Step 4: Explode the values from each key into separate rows
df_exploded_values = df_exploded.select("Key", explode("Values").alias("Value"))

# Step 5: Get all distinct values across the exploded "Value" column
distinct_values = df_exploded_values.select("Value").distinct()

# Collect distinct values into a Python list, making sure to handle None values
distinct_values_list = [row["Value"] for row in distinct_values.collect() if row["Value"] is not None]

for value in distinct_values_list:
    # Create a new column for each distinct value
    df_parsed= df_parsed.withColumn(
        f"flag_{value.replace(' ', '_')}", 
        (array_contains(col("cleaned_tagged_description").getItem("Occasion"), value) | 
         array_contains(col("cleaned_tagged_description").getItem("Age Range"), value)).cast("boolean").cast("int")
    )


# Display the resulting DataFrame with flags
display(df_parsed)

# COMMAND ----------

df_parsed.write.format("delta").mode("overwrite").saveAsTable("shared.lego.gold_data_LLM_tagging")

# COMMAND ----------


