# lego-data-modelling


## [Ingestion Notebook]

This notebook demonstrates a data processing workflow using PySpark, focusing on manipulating and saving a dataset related to product information. The key steps include:

1. **Data Concatenation**: Combining multiple columns (`headline_text`, `product_name`, `product_description`, `category_name`, and `site_product_identifier`) into a single column named `concatenated_text` for a more streamlined dataset.
2. **Data Storage**: Saving the processed dataset in a Delta format into a table named `shared.lego.silver_data`, ensuring that the data is readily available for further analysis or reporting.

The workflow is designed to enhance data readability and accessibility, making it easier to perform subsequent data analysis tasks.