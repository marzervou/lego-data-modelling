# Databricks notebook source
# MAGIC %run /Workspace/Users/maria.zervou@databricks.com/lego-data-modelling/data_science/config.py

# COMMAND ----------

catalog = config['catalog']
db = config['dbName']

# COMMAND ----------

# For this first basic demo, we'll keep the configuration as a minimum. In real app, you can make all your RAG as a param (such as your prompt template to easily test different prompts!)
chain_config = {
    "llm_model_serving_endpoint_name": "databricks-meta-llama-3-1-405b-instruct",  # the foundation model we want to use
    "llm_prompt_template": """You are an assistant that tags Lego products. Given the product description {PRODUCT_DESCRIPTION} tag the description against all of the following categories:
    - Occasion: (Christmas Gift, Birthday Gift, Holiday Gift, Everyday Play, Special Treat)
    - Age Range: (Preschool, Kids, Teenagers, Adults)
    - Theme: (Adventure, Animals, Cars and Trucks, Fantasy, Space, Superheroes)
    - Skill Level: (Beginner, Intermediate, Advanced)
    - Play Style:(Building and Construction, Imaginative Play, Action and Adventure, Strategy and Puzzle-Solving)
    - Franchise:(Star Wars, Harry Potter, Marvel, DC Comics, Disney, Pixar)
    - Product Type:(Building Sets, Vehicles, Minifigures, Accessories, Books and Comics)
      Return the result in a JSON format ONLY do not add more text. If it unclear or not explicitly mentioned in the description return []
     """
}

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

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


df_concatenated = spark.read.format("delta").table("shared.lego.silver_data")

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


