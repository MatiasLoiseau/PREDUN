{{ config(materialized='table', full_refresh=true) }}

with params as (
    -- Anclado a la fecha máxima de los datos (no a current_date) para que el
    -- estado sea reproducible: depende del snapshot de datos, no del día de ejecución.
    select
        max(trim(fecha)::date)                      as data_max_date,
        max(trim(fecha)::date) - interval '2 years' as cutoff_date
    from {{ ref('cursada_historica') }}
    where trim(coalesce(fecha, '')) ~ '^\d{4}-\d{2}-\d{2}$'
),

ultima_cursada as (
    select
        legajo,
        max(fecha)::date  as fecha_ultima_cursada
    from {{ ref('cursada_historica') }}
    group by legajo
),

finalizacion_estimada as (
    select distinct
        legajo,
        'finalizacion_estimada' as status
    from {{ ref('porcentaje_avance') }}
    where coalesce(
        case
            when trim(porcentaje_avance) ~ '^\d+([.,]\d+)?$'
            then replace(trim(porcentaje_avance), ',', '.')::numeric
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
    left join finalizacion_estimada g using (legajo)
),

status_enriched as (
    select
        t.legajo,
        t.fecha_ultima_cursada,
        case
            when t.status = 'finalizacion_estimada'      then 'finalizacion_estimada'
            when t.fecha_ultima_cursada is null          then 'abandonó'
            when t.fecha_ultima_cursada < p.cutoff_date  then 'abandonó'
            else                                              'estudiando'
        end                                             as status,
        case
            when t.status = 'finalizacion_estimada'      then 0
            when t.fecha_ultima_cursada is null          then 1
            when t.fecha_ultima_cursada < p.cutoff_date  then 1
            else                                              0
        end::int                                        as dropout_flag,
        current_timestamp                               as inserted_at
    from todos t
    cross join params p
)

select distinct *
from status_enriched