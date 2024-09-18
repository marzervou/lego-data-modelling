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
  
VECTOR_SEARCH_ENDPOINT_NAME="dbdemos_vs_endpoint"
config['VECTOR_SEARCH_ENDPOINT_NAME'] = "dbdemos_vs_endpoint"

DATABRICKS_SITEMAP_URL = "https://docs.databricks.com/en/doc-sitemap.xml"

config['catalog'] = "shared"
catalog = "shared"
dbName = db = "lego"
config['dbName'] = "lego"
