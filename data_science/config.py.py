# Databricks notebook source
# MAGIC %md 
# MAGIC ## Configuration file
# MAGIC
# MAGIC Please change your catalog and schema here to run the demo on a different catalog.
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-science&org_id=1444828305810485&notebook=%2Fconfig&demo_name=llm-rag-chatbot&event=VIEW&path=%2F_dbdemos%2Fdata-science%2Fllm-rag-chatbot%2Fconfig&version=1">

# COMMAND ----------

if 'config' not in locals():
  config = {}


raw_data_path = "/Volumes/shared/lego/raw_data/product_catalogue_categories.csv"
config['raw_data_path'] = raw_data_path
config['silver_data'] = "silver_data"
config['gold_data'] = "gold_data"
config['gold_data_llm'] = "gold_data_llm"
config['dbName'] = "lego"
config['catalog'] = "shared"


config['llm_model_serving_endpoint_name'] = "databricks-mixtral-8x7b-instruct"

config['VECTOR_SEARCH_ENDPOINT_NAME'] = "dbdemos_vs_endpoint"


catalog = "shared"
dbName = db = "lego"
