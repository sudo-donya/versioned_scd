/*see minimum requred configuration in schema.yml */
select
    id
    , component_name
    , label
    , amount
    , date_of_birth
    , component_description
    , source_system_updated_date
    , staging_load_date
from {{ ref('int_scd2_plus_staging_data') }}
order by 1, 7, 8
