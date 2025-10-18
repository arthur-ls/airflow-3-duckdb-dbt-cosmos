{% macro incremental_config(primary_key) %}
{{ 
    config(
        materialized='incremental',
        table_type='iceberg',
        unique_key=primary_key,
        format='parquet',                    
        on_schema_change='sync_all_columns',
    )
}}
{% endmacro %}