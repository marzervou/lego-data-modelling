# Databricks notebook source
# MAGIC %pip install -U --quiet databricks-sdk==0.28.0 databricks-agents mlflow-skinny mlflow mlflow[gateway] databricks-vectorsearch langchain==0.2.1 langchain_core==0.2.5 langchain_community==0.2.4
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./_resources/00-init

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE `shared`.`lego`.`gold_data_regex` 
# MAGIC SET TBLPROPERTIES (delta.enableChangeDataFeed = true)

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- Loading the data from the 'Create Data' notebook
# MAGIC SELECT * FROM `shared`.`lego`.`gold_data_regex`

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient

# Initialize the Vector Search Client with the option to disable the notice.
vsc = VectorSearchClient(disable_notice=True)

# Define the name of the Vector Search endpoint.
VECTOR_SEARCH_ENDPOINT_NAME = config['VECTOR_SEARCH_ENDPOINT_NAME']

# Check if the Vector Search endpoint already exists.
if not endpoint_exists(vsc, VECTOR_SEARCH_ENDPOINT_NAME):
    # If the endpoint does not exist, create a new one with the specified name and type.
    vsc.create_endpoint(name=VECTOR_SEARCH_ENDPOINT_NAME, endpoint_type="STANDARD")

# Wait for the Vector Search endpoint to be fully operational.
wait_for_vs_endpoint_to_be_ready(vsc, VECTOR_SEARCH_ENDPOINT_NAME)

# Print a confirmation message indicating the endpoint is ready for use.
print(f"Endpoint named {VECTOR_SEARCH_ENDPOINT_NAME} is ready.")

# COMMAND ----------

# Import necessary libraries for working with Databricks SDK and catalog services.
from databricks.sdk import WorkspaceClient
import databricks.sdk.service.catalog as c

# Define the catalog and database names for clarity and reusability.
catalog = config['catalog']
db = config['dbName']

# Construct the full names for the source table and the vector search index using the catalog and database names.
source_table_fullname = f"{catalog}.{db}.gold_data_regex"
vs_index_fullname = f"{catalog}.{db}.gold_data_regex_index"

# Check if the vector search index already exists on the specified endpoint.
if not index_exists(vsc, VECTOR_SEARCH_ENDPOINT_NAME, vs_index_fullname):
  # If the index does not exist, print a message indicating the creation of the index.
  print(f"Creating index {vs_index_fullname} on endpoint {VECTOR_SEARCH_ENDPOINT_NAME}...")
  
  # Create a new delta sync index on the vector search endpoint.
  # This index is created from a source Delta table and is kept in sync with the source table.
  vsc.create_delta_sync_index(
    endpoint_name=VECTOR_SEARCH_ENDPOINT_NAME,  # The name of the vector search endpoint.
    index_name=vs_index_fullname,  # The name of the index to create.
    source_table_name=source_table_fullname,  # The full name of the source Delta table.
    pipeline_type="TRIGGERED",  # The type of pipeline to keep the index in sync with the source table.
    primary_key="site_product_identifier",  # The primary key column of the source table.
    embedding_source_column='concatenated_text',  # The column to use for generating embeddings.
    embedding_model_endpoint_name='databricks-gte-large-en'  # The name of the embedding model endpoint.
  )

  # Wait for the index to be fully operational before proceeding.
  wait_for_index_to_be_ready(vsc, VECTOR_SEARCH_ENDPOINT_NAME, vs_index_fullname)
else:
  # If the index already exists, wait for it to be ready before syncing.
  wait_for_index_to_be_ready(vsc, VECTOR_SEARCH_ENDPOINT_NAME, vs_index_fullname)

  # Sync the existing index with the latest data from the source table.
  vsc.get_index(VECTOR_SEARCH_ENDPOINT_NAME, vs_index_fullname).sync()

# Print a confirmation message indicating the index is ready for use.
print(f"index {vs_index_fullname} on table {source_table_fullname} is ready")

# COMMAND ----------

# Similarity Search

# Define the query text for the similarity search.
query_text = "explore the world in miniature  world of wonders "

# Perform a similarity search on the vector search index.
# The search uses the query text to find similar entries based on the specified columns.
# Filters can be applied to narrow down the search results, but are commented out in this example.
results = vsc.get_index(VECTOR_SEARCH_ENDPOINT_NAME, vs_index_fullname).similarity_search(
  query_text=query_text,
  columns=['product_name', 'category_name', 'product_description', 'play_type', 'occasion_type','product_type','theme_type'],
  num_results=3)  # Specify the number of results to return.

# Extract the search results from the response.
docs = results.get('result', {}).get('data_array', [])
# The 'docs' variable now contains the search results, ready for further processing or display.
docs

# COMMAND ----------

# DBTITLE 1,Test cases for vector search
# play with the 3-in-1 forest monkey with his banana and toucan 
# protect the king’s treasure hidden deep in the knights’ castle!

# COMMAND ----------

protect the king’s treasure hidden deep in the knights’ castle! |
