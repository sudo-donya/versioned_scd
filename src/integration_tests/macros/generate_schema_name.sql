{%- macro set_schema_name(custom_schema_name, node) -%}
{%- if node.resource_type == "seed" -%}
{%- if node.fqn[:-1]|length < 2 -%}
            {%- set custom_schema_name = 'seed_data' -%}
        {%- else -%}
            {% set custom_schema_name = node.config.schema ~ '_' ~ node.fqn[1] %}
        {%- endif -%}
    {%- elif node.resource_type == "model" -%}
{%- if node.fqn[1] in ["intermediate", "marts"] -%}
            {%- set custom_schema_name = node.config.schema -%}
        {%- elif node.fqn[:-1]|length == 1 -%}
{% set custom_schema_name = node.fqn[1] %}
{%- endif -%}
{%- endif -%}
{{ custom_schema_name }}
{%- endmacro -%}

{%- macro set_schema_name_test_env(schema_name) -%}
{%- set pr_id = env_var('PR_ID', 'not-set') -%}
{%- set build_id = env_var('BUILD_ID', 'not-set') -%}

{%- if target.name == 'test' and pr_id|int -%}
        {%- set schema_name = 'pr_' ~ pr_id ~ '_' ~ schema_name -%}
    {%- elif target.name == 'test' and build_id|int -%}
{%- set schema_name = 'build_' ~ build_id ~ '_' ~ schema_name -%}
{%- endif -%}
{{ schema_name }}
{%- endmacro -%}

{%- macro set_project_name_prefix(schema_name, node) -%}
{# {%- if node.fqn[1] not in ["marts", "utilities"]  -%} #}
{%- if node.config.database != 'analytics' -%}
{%- set schema_name = project_name ~ '_' ~ schema_name -%}
{%- endif -%}
{{ schema_name }}
{%- endmacro -%}

{%- macro set_schema_name_dev_env(custom_schema_name, node) -%}
{%- if target.name in ["dev", "development"] and node.fqn[1] == "marts" -%}
{% set custom_schema_name = custom_schema_name %}
{%- endif -%}
{{ custom_schema_name }}
{%- endmacro -%}

{% macro generate_schema_name(custom_schema_name, node) -%}

{%- if custom_schema_name is none or target.name not in ['production', 'prod', 'dev', 'development', 'test'] -%}
        {%- set schema_name = target.schema -%}
    {%- else -%}
{%- set custom_schema_name = set_schema_name(custom_schema_name, node) -%}
{%- set custom_schema_name = set_schema_name_dev_env(custom_schema_name, node) -%}
{%- set schema_name = custom_schema_name -%}
{#{ custom_schema_name }}_{{ default_schema | trim }#}
{%- endif -%}

{%- if target.name != 'personal' -%}
{%- set schema_name = set_project_name_prefix(schema_name, node) -%}
{%- endif -%}
{%- set schema_name = set_schema_name_test_env(schema_name) -%}
{{ schema_name }}
-- noqa: LT05
{# {{ print(node['config']['schema'] ~ ' - ' ~ node.fqn ~ ' - ' ~ node.resource_type ~ ' - ' ~ custom_schema_name ~ '   ' ~ node.fqn[1:-1]|length) }} #}
{%- endmacro %}
