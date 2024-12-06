{% macro generate_database_name(custom_database_name, node) -%}
    {%- set default_database = target.database -%}
    {%- if custom_database_name is none -%}
        {%- set target_database = default_database -%}
    {%- else -%}
        {%- set target_database = custom_database_name | trim -%}
    {%- endif -%}

    {%- if target.name in ["test"] -%}
        test_{{ target_database }}
    {%- elif target.name in ["production"] -%}
        {{ target_database }}
    {%- else -%}
        {{ default_database }}
    {%- endif -%}

{%- endmacro %}
