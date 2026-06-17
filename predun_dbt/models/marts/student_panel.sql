{{ config(
    materialized = 'table',
    full_refresh = true,
    schema       = 'marts',
    tags         = ['mart', 'student', 'dropout']
) }}

/*
  Panel longitudinal con etiquetas point-in-time.

  UNIDAD DE ANÁLISIS: (legajo, período académico) — una fila por estudiante-período.
    Las features se agregan sobre TODAS las carreras del legajo (actividad
    institucional). La carrera activa (cod_carrera) se conserva como atributo
    descriptivo/categórico mediante carry-forward de la carrera con actividad
    más reciente hasta t. Esto unifica la etiqueta y las features en el mismo
    nivel (legajo), eliminando la formulación híbrida anterior en la que las
    features eran por carrera y la etiqueta por legajo.

  Variable objetivo (dropout_next):
    - Horizonte de abandono: 4 períodos (2 años) sin actividad académica
      institucional posterior al período t, consistente con la definición
      operacional de abandono del sistema (student_status).
    - Censura a la derecha ESTRICTA y asimétrica:
        * Si los 4 períodos futuros NO son observables  -> NULL (censura).
        * Si se observa actividad en la ventana futura   -> 0 (el estudiante
          retomó dentro del horizonte; no es abandono).
        * Si el legajo graduó (avance >= 90%)            -> 0.
        * Si los 4 períodos futuros están observados y    -> 1 (abandono).
          sin actividad.
      Solo se emite una etiqueta positiva cuando los 4 períodos futuros están
      plenamente observados, evitando positivos prematuros.
    - Finalización curricular estimada: el porcentaje de avance disponible es un
      único snapshot (no historizado), por lo que la exclusión por finalización
      estimada NO es estrictamente point-in-time. Se aplica solo dentro de la
      ventana plenamente observada para acotar el sesgo. Limitación documentada:
      requiere padrón oficial de egreso con fecha para corregirse.

  Conjunto de riesgo (at_risk):
    - Una fila está "en riesgo" en el período t si el legajo registró actividad
      académica (en cualquier carrera) dentro de los 4 períodos que terminan en t,
      es decir, si NO se encuentra ya en estado de abandono según la regla de 2 años.
      Entrenamiento y scoring se restringen a at_risk = 1.

  Calendario dinámico: cada trayectoria comienza en su primer período con
  actividad y se extiende hasta el último período con actividad en
  canonical.cursada_historica.
*/

with cursadas_dated as (

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
    from cursadas_dated
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

-- 2) Actividad institucional por legajo en cada período del calendario
actividad_legajo as (
    select distinct legajo, period_start
    from cursadas_dated
),

-- 3) Finalización curricular estimada: porcentaje de avance final >= 90% (snapshot, ver header)
finalizacion_estimada as (
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
        from cursadas_dated
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
        activo_en_periodo,
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
        le.period_start,
        le.activo_en_periodo,
        le.at_risk,
        case
            when le.n_periodos_futuros_obs < 4      then null  -- censura: ventana futura incompleta
            when coalesce(le.max_act_futura, 0) = 1 then 0     -- retomó dentro del horizonte
            when g.legajo is not null               then 0     -- finalización curricular estimada: nunca abandono
            else 1                                             -- 4 períodos futuros sin actividad
        end as dropout_next
    from legajo_estado le
    left join finalizacion_estimada g
      on g.legajo = le.legajo
),

-- 5) Carrera activa (atributo descriptivo) por carry-forward point-in-time:
--    carrera con más cursadas en el período activo más reciente hasta t.
carrera_dominante as (
    select distinct on (legajo, period_start)
        legajo, period_start, cod_carrera
    from (
        select legajo, cod_carrera, period_start, count(*) as n
        from cursadas_dated
        group by 1, 2, 3
    ) z
    order by legajo, period_start, n desc, cod_carrera
),

carrera_ff as (
    -- último valor no nulo (last non-null) vía agrupamiento por conteo acumulado:
    -- dentro de cada grupo (definido por la llegada de un valor no nulo) el
    -- max() recupera la única carrera presente, propagándola hacia adelante.
    select
        legajo,
        period_start,
        max(cod_carrera) over (partition by legajo, grp) as cod_carrera
    from (
        select
            lg.legajo,
            lg.period_start,
            cd.cod_carrera,
            count(cd.cod_carrera) over (
                partition by lg.legajo order by lg.period_start
                rows between unbounded preceding and current row
            ) as grp
        from legajo_grid lg
        left join carrera_dominante cd
          on  cd.legajo       = lg.legajo
          and cd.period_start = lg.period_start
    ) s
),

-- 6) Métricas por período agregadas sobre TODAS las carreras del legajo
--    (según el término académico declarado anio/tipo_cursada)
cursadas_periodo as (
    select
        legajo,
        concat(anio, '_', tipo_cursada)                            as academic_period,
        count(*)                                                   as materias_en_periodo,
        sum(case when resultado ilike 'Promoc%' then 1 else 0 end) as promo_en_periodo,
        avg(nullif(nota, '')::numeric)                             as nota_media_en_periodo
    from {{ ref('cursada_historica') }}
    group by 1, 2
),

-- 7) Panel con features de ventana móvil, acumulado y recencia institucional
panel_raw as (
    select
        lb.legajo,
        cf.cod_carrera,
        lb.academic_period,
        lb.at_risk,
        lb.dropout_next,

        /* ---------- features dentro del período ---------- */
        coalesce(cp.materias_en_periodo, 0) as materias_en_periodo,
        coalesce(cp.promo_en_periodo,    0) as promo_en_periodo,
        cp.nota_media_en_periodo,

        /* ---------- ventana móvil de 4 períodos ---------- */
        sum(coalesce(cp.materias_en_periodo, 0)) over w as materias_win3,
        sum(coalesce(cp.promo_en_periodo,    0)) over w as promo_win3,
        avg(cp.nota_media_en_periodo)            over w as nota_win3,

        /* ---------- progreso acumulado (calculado en dbt, no en Python) ---------- */
        sum(coalesce(cp.materias_en_periodo, 0)) over wcum as materias_cum,

        /* ---------- tasas de promoción normalizadas (en dbt) ---------- */
        case when coalesce(cp.materias_en_periodo, 0) > 0
             then cp.promo_en_periodo::numeric / cp.materias_en_periodo
             else null end                       as promo_rate_period,
        case when sum(coalesce(cp.materias_en_periodo, 0)) over w > 0
             then sum(coalesce(cp.promo_en_periodo, 0)) over w::numeric
                  / sum(coalesce(cp.materias_en_periodo, 0)) over w
             else null end                       as promo_rate_win3,

        /* ---------- recencia institucional: días desde el último período
                      con actividad del legajo (en cualquier carrera) ---------- */
        (lb.period_start
         - max(case when lb.activo_en_periodo = 1 then lb.period_start end)
               over wcum)::int                    as dias_desde_ult_actividad

    from labels lb
    left join carrera_ff cf
           on  cf.legajo       = lb.legajo
           and cf.period_start = lb.period_start
    left join cursadas_periodo cp
           on  cp.legajo          = lb.legajo
           and cp.academic_period = lb.academic_period
    window
        w as (partition by lb.legajo order by lb.period_start
              rows between 3 preceding and current row),
        wcum as (partition by lb.legajo order by lb.period_start
                 rows between unbounded preceding and current row)
)

-- 8) Panel final: una fila por (legajo, período), con estado de riesgo y etiqueta
--    (NULL = censurada). El entrenamiento filtra at_risk = 1 AND dropout_next IS NOT NULL;
--    el scoring filtra at_risk = 1 y toma la fila más reciente por legajo.
select
    legajo,
    cod_carrera,
    academic_period,
    materias_en_periodo,
    promo_en_periodo,
    nota_media_en_periodo,
    materias_win3,
    promo_win3,
    nota_win3,
    materias_cum,
    promo_rate_period,
    promo_rate_win3,
    dias_desde_ult_actividad,
    at_risk,
    dropout_next
from panel_raw
