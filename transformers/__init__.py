"""
transformers/

Package containing data transformation logic for the GitHub ETL pipeline.
Each sub-package corresponds to one data source connector.
Transformers receive raw extracted data and return cleaned Polars DataFrames
ready for schema validation and warehouse loading.
"""
