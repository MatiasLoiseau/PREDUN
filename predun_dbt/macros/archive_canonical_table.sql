{% macro archive_canonical_table(this) %}

    {% set history_identifier = this.identifier ~ '_history' %}
    {% set history_relation = adapter.get_relation(
           database=this.database,
           schema=this.schema,
           identifier=history_identifier) %}

    {% if history_relation is none %}
        create table {{ this.schema }}.{{ history_identifier }}
        (
            like {{ this }}
            including defaults
            including comments
            including storage
        );
        alter table {{ this.schema }}.{{ history_identifier }}
            add primary key (row_hash, academic_period);
    {% endif %}

    {% if adapter.get_relation(
            database=this.database,
            schema=this.schema,
            identifier=this.identifier) is not none %}
        insert into {{ this.schema }}.{{ history_identifier }}
        select *
        from {{ this }}
        on conflict (row_hash, academic_period)
        do nothing;
    {% endif %}
{% endmacro %}