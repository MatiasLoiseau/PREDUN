{% macro get_latest_tag(table_name) %}
    (
        select max(academic_period)
        from {{ table_name }}
    )
{% endmacro %}