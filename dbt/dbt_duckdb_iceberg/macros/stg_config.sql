{% macro stg_config() %}
  {{
    config(
      materialized = 'view'
    )
  }}
{% endmacro %}