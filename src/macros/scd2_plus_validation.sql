{% test scd2_plus_validation(model,unique_key='id',scd_valid_from_col_name='valid_from',scd_valid_to_col_name='valid_to') %}

    with data as (
        select
         {{ unique_key }},
         {{ scd_valid_from_col_name }} ,
         {{ scd_valid_to_col_name }} ,
         lag({{ scd_valid_to_col_name }}) over(partition by {{ unique_key }} order by coalesce({{ scd_valid_from_col_name }}, '1900-01-01')) prev_{{ scd_valid_to_col_name }}
        from {{ model }}
    )
    select *
    from data
    where ({{ scd_valid_from_col_name }}>{{ scd_valid_to_col_name }}) /*valid from must always be less then valid to */
      or  ({{ scd_valid_from_col_name }}<>coalesce(prev_{{ scd_valid_to_col_name }} ,{{ scd_valid_from_col_name }})) /*prev valid to must always be valid from or null (1st record) */

{% endtest %}
