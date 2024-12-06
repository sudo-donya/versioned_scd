with source as (
    select *
    from {{ source('integration_tests_seed_data', 'scd2_plus_staging_data') }}
)

, renamed as (
    select
        id
        , "Name" as component_name
        , "Label" as label
        , "Description" as component_description
        , "Amount" as amount
        , "BirthdayDate" as date_of_birth
        , "SourceSystem_UpdatedDate" as source_system_updated_date
        , "Staging_LoadDate" as staging_load_date
        , "Comment" as comment
    from source
)

select * from renamed
