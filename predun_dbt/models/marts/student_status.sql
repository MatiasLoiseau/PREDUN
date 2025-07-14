{{ config(materialized='table', full_refresh=true) }}

with params as (
    select
        current_date                       as today,
        current_date - interval '2 years'  as cutoff_date
),

ultima_cursada as (
    select
        legajo,
        max(fecha)::date  as fecha_ultima_cursada
    from {{ ref('cursada_historica') }}
    group by legajo
),

graduados as (
    select distinct
        legajo,
        'graduado'       as status
    from {{ ref('porcentaje_avance') }}
    where coalesce(
        case
            when trim(porcentaje_avance) ~ '^\d+(\.\d+)?$'
            then porcentaje_avance::numeric
            else null
        end,
        0
    ) >= 90
),

todos as (
    select
        a.legajo,
        u.fecha_ultima_cursada,
        g.status
    from {{ ref('alumnos') }} a
    left join ultima_cursada u using (legajo)
    left join graduados     g using (legajo)
),

status_enriched as (
    select
        t.legajo,
        t.fecha_ultima_cursada,
        case
            when t.status = 'graduado'                   then 'graduado'
            when t.fecha_ultima_cursada is null          then 'abandonó'
            when t.fecha_ultima_cursada < p.cutoff_date  then 'abandonó'
            else                                              'estudiando'
        end                                             as status,
        case
            when t.status = 'graduado'                   then 0
            when t.fecha_ultima_cursada is null          then 1
            when t.fecha_ultima_cursada < p.cutoff_date  then 1
            else                                              0
        end::int                                        as dropout_flag,
        current_timestamp                               as inserted_at
    from todos t
    cross join params p
)

select *
from status_enriched