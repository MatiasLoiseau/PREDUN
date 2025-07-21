{{ config(
    materialized = 'table',
    full_refresh = true,
    schema       = 'marts',
    tags         = ['mart', 'student', 'dropout']
) }}

-- 1) Calendario académico 2011-1C … 2025-2C
with periods as (
    select
        gs.period_start::date                                                as period_start,
        concat(
            date_part('year', gs.period_start)::int,
            '_',
            case
                when date_part('month', gs.period_start) = 3 then '1C'
                else '2C'
            end
        )                                                                    as academic_period
    from generate_series(
             '2011-03-01'::date,
             '2025-09-01'::date,
             interval '6 months'
         ) as gs(period_start)
),

-- 2) Todas las combinaciones estudiante-carrera
base as (
    select distinct
        legajo,
        cod_carrera
    from {{ ref('cursada_historica') }}
),

-- 3) Explosión estudiante × carrera × período
student_periods as (
    select
        b.legajo,
        b.cod_carrera,
        p.academic_period,
        p.period_start
    from base b
    cross join periods p
),

-- 4) Métricas por período (dentro del mismo cuatrimestre)
cursadas_periodo as (
    select
        legajo,
        cod_carrera,
        concat(anio, '_', tipo_cursada)                    as academic_period,
        count(*)                                           as materias_en_periodo,
        sum(case when resultado ilike 'Promoc%' then 1 else 0 end) as promo_en_periodo,
        avg(nullif(nota,'')::numeric)                      as nota_media_en_periodo
    from {{ ref('cursada_historica') }}
    group by 1, 2, 3
),

-- 5) Etiqueta de abandono (flag calculado en un mart previo)
status as (
    select
        legajo,
        dropout_flag::int                                  as dropout_flag
    from {{ ref('student_status') }}
),

-- 6) Panel crudo con ventanas y label
panel_raw as (
    select
        sp.legajo,
        sp.cod_carrera,
        sp.academic_period,

        /* ---------- features dentro del período ---------- */
        coalesce(cp.materias_en_periodo, 0)                as materias_en_periodo,
        coalesce(cp.promo_en_periodo,    0)                as promo_en_periodo,
        cp.nota_media_en_periodo,

        /* ---------- ventana móvil de 3 períodos ---------- */
        sum(coalesce(cp.materias_en_periodo, 0))
            over w                                         as materias_win3,
        sum(coalesce(cp.promo_en_periodo, 0))
            over w                                         as promo_win3,
        avg(cp.nota_media_en_periodo)
            over w                                         as nota_win3,

        /* ---------- recencia corregida ------------------- */
        (sp.period_start
         - lag(sp.period_start) over (partition by sp.legajo, sp.cod_carrera
                                      order by sp.period_start))::int
                                                         as dias_desde_ult_periodo,

        /* ---------- label (abandona al siguiente período) */
        lead(status.dropout_flag) over (partition by sp.legajo, sp.cod_carrera
                                        order by sp.period_start)
                                                         as dropout_next
    from student_periods sp
    left join cursadas_periodo cp
           on  cp.legajo          = sp.legajo
           and cp.cod_carrera     = sp.cod_carrera
           and cp.academic_period = sp.academic_period
    left join status
           on  status.legajo      = sp.legajo
    window w as (
        partition by sp.legajo, sp.cod_carrera
        order by     sp.period_start
        rows between 3 preceding and current row
    )
)

-- 7) Guarda sólo filas con label definida
select *
from   panel_raw
where  dropout_next is not null