{{ config(
    materialized = 'table',
    full_refresh = true,
    schema       = 'marts',
    tags         = ['mart', 'student', 'dropout']
) }}

/*
  Panel longitudinal con etiquetas point-in-time.

  Variable objetivo (dropout_next):
    - Horizonte de abandono: 4 períodos (2 años) sin actividad académica
      posterior al período t, consistente con la definición operacional
      de abandono del sistema (student_status).
    - Censura a la derecha: la etiqueta se define solo si al menos 2 de los
      4 períodos futuros son observables en los datos; dropout_next es NULL
      en caso contrario y la fila queda excluida del entrenamiento.
    - Graduados (porcentaje de avance >= 90%) nunca se etiquetan como abandono.

  Conjunto de riesgo (at_risk):
    - Una fila está "en riesgo" en el período t si el legajo registró actividad
      académica (en cualquier carrera) dentro de los 4 períodos que terminan
      en t, es decir, si NO se encuentra ya en estado de abandono según la
      regla de 2 años. Entrenamiento y scoring se restringen a at_risk = 1.

  Cada trayectoria (legajo, carrera) comienza en su primer período con
  actividad registrada. El calendario es dinámico: se extiende hasta el
  último período con actividad en canonical.cursada_historica.
*/

with cursadas_mapped as (

    -- Asignación de cada cursada a un período del calendario por su fecha real.
    -- Cubre también los términos especiales (verano, anual), cuyo identificador
    -- anio/tipo_cursada no corresponde a ningún cuatrimestre 1C/2C.
    select
        legajo,
        cod_carrera,
        case
            when extract(month from trim(fecha)::date) between 3 and 8
                then make_date(extract(year from trim(fecha)::date)::int, 3, 1)
            when extract(month from trim(fecha)::date) >= 9
                then make_date(extract(year from trim(fecha)::date)::int, 9, 1)
            else make_date(extract(year from trim(fecha)::date)::int - 1, 9, 1)
        end as period_start
    from {{ ref('cursada_historica') }}
    where trim(coalesce(fecha, '')) ~ '^\d{4}-\d{2}-\d{2}$'
),

data_bounds as (
    select max(period_start) as last_period_start
    from cursadas_mapped
),

-- 1) Calendario académico dinámico: 2011-1C hasta el último período con datos
periods as (
    select
        gs.period_start::date as period_start,
        concat(
            date_part('year', gs.period_start)::int,
            '_',
            case when date_part('month', gs.period_start) = 3 then '1C' else '2C' end
        ) as academic_period
    from generate_series(
             '2011-03-01'::date,
             '2035-09-01'::date,
             interval '6 months'
         ) as gs(period_start)
    cross join data_bounds db
    where gs.period_start <= db.last_period_start
),

-- 2) Actividad por legajo y por (legajo, carrera) en cada período del calendario
actividad_legajo as (
    select distinct legajo, period_start
    from cursadas_mapped
),

actividad_carrera as (
    select distinct legajo, cod_carrera, period_start
    from cursadas_mapped
),

-- 3) Graduados según porcentaje de avance final >= 90%
graduados as (
    select distinct legajo
    from {{ ref('porcentaje_avance') }}
    where coalesce(
        case
            when trim(porcentaje_avance) ~ '^\d+([.,]\d+)?$'
            then replace(trim(porcentaje_avance), ',', '.')::numeric
            else null
        end, 0) >= 90
),

-- 4) Grilla por legajo desde su primera actividad: estado y etiqueta point-in-time
legajo_grid as (
    select
        lf.legajo,
        p.academic_period,
        p.period_start,
        case when al.legajo is not null then 1 else 0 end as activo_en_periodo
    from (
        select legajo, min(period_start) as first_ps
        from cursadas_mapped
        group by 1
    ) lf
    join periods p
      on p.period_start >= lf.first_ps
    left join actividad_legajo al
      on  al.legajo       = lf.legajo
      and al.period_start = p.period_start
),

legajo_estado as (
    select
        legajo,
        academic_period,
        period_start,
        case when max(activo_en_periodo) over w_past = 1 then 1 else 0 end as at_risk,
        max(activo_en_periodo) over w_fut as max_act_futura,
        count(*)               over w_fut as n_periodos_futuros_obs
    from legajo_grid
    window
        w_past as (partition by legajo order by period_start
                   rows between 3 preceding and current row),
        w_fut  as (partition by legajo order by period_start
                   rows between 1 following and 4 following)
),

labels as (
    select
        le.legajo,
        le.academic_period,
        le.at_risk,
        case
            when le.n_periodos_futuros_obs < 2      then null  -- ventana futura insuficiente (censura)
            when g.legajo is not null               then 0     -- graduado: nunca abandono
            when coalesce(le.max_act_futura, 0) = 0 then 1     -- sin actividad en el horizonte
            else 0
        end as dropout_next
    from legajo_estado le
    left join graduados g
      on g.legajo = le.legajo
),

-- 5) Trayectorias estudiante-carrera desde su primera actividad
base as (
    select legajo, cod_carrera, min(period_start) as first_ps
    from cursadas_mapped
    group by 1, 2
),

student_periods as (
    select
        b.legajo,
        b.cod_carrera,
        p.academic_period,
        p.period_start
    from base b
    join periods p
      on p.period_start >= b.first_ps
),

-- 6) Métricas por período (según el término académico declarado anio/tipo_cursada)
cursadas_periodo as (
    select
        legajo,
        cod_carrera,
        concat(anio, '_', tipo_cursada)                            as academic_period,
        count(*)                                                   as materias_en_periodo,
        sum(case when resultado ilike 'Promoc%' then 1 else 0 end) as promo_en_periodo,
        avg(nullif(nota, '')::numeric)                             as nota_media_en_periodo
    from {{ ref('cursada_historica') }}
    group by 1, 2, 3
),

-- 7) Panel con features de ventana móvil y recencia real
panel_raw as (
    select
        sp.legajo,
        sp.cod_carrera,
        sp.academic_period,

        /* ---------- features dentro del período ---------- */
        coalesce(cp.materias_en_periodo, 0) as materias_en_periodo,
        coalesce(cp.promo_en_periodo,    0) as promo_en_periodo,
        cp.nota_media_en_periodo,

        /* ---------- ventana móvil de 4 períodos ---------- */
        sum(coalesce(cp.materias_en_periodo, 0)) over w as materias_win3,
        sum(coalesce(cp.promo_en_periodo,    0)) over w as promo_win3,
        avg(cp.nota_media_en_periodo)            over w as nota_win3,

        /* ---------- recencia real: días desde el último período
                      con actividad en esta carrera ---------- */
        (sp.period_start
         - max(case when ac.legajo is not null then sp.period_start end)
               over (partition by sp.legajo, sp.cod_carrera
                     order by sp.period_start
                     rows between unbounded preceding and current row))::int
                                            as dias_desde_ult_actividad
    from student_periods sp
    left join cursadas_periodo cp
           on  cp.legajo          = sp.legajo
           and cp.cod_carrera     = sp.cod_carrera
           and cp.academic_period = sp.academic_period
    left join actividad_carrera ac
           on  ac.legajo       = sp.legajo
           and ac.cod_carrera  = sp.cod_carrera
           and ac.period_start = sp.period_start
    window w as (
        partition by sp.legajo, sp.cod_carrera
        order by     sp.period_start
        rows between 3 preceding and current row
    )
)

-- 8) Panel final: todas las filas, con estado de riesgo y etiqueta (NULL = censurada).
--    El entrenamiento filtra at_risk = 1 AND dropout_next IS NOT NULL;
--    el scoring filtra at_risk = 1 y toma la fila más reciente por legajo.
select
    pr.legajo,
    pr.cod_carrera,
    pr.academic_period,
    pr.materias_en_periodo,
    pr.promo_en_periodo,
    pr.nota_media_en_periodo,
    pr.materias_win3,
    pr.promo_win3,
    pr.nota_win3,
    pr.dias_desde_ult_actividad,
    lb.at_risk,
    lb.dropout_next
from panel_raw pr
join labels lb
  on  lb.legajo          = pr.legajo
  and lb.academic_period = pr.academic_period
