{{ config(
    materialized = 'table',
    full_refresh = true,
    pre_hook = ["{{ archive_canonical_table(this) }}"],
    post_hook = [
        'alter table {{ this }} add primary key (evento_hash)'
    ]
) }}

/*
  Fuente de verdad del historial de cursadas. Deduplica en DOS niveles.

  (1) Entre entregas: cada entrega institucional es un snapshot completo del
      historial, así que el modelo se construye solo desde la entrega más
      reciente (`get_latest_tag`). Las anteriores quedan archivadas por el
      pre_hook.

  (2) Dentro de la entrega, a nivel de EVENTO ACADÉMICO. Desde 2021_1C la
      exportación emite dos actas por el mismo evento: una con `origen='R'`
      (trae `fecha_vigencia` = fecha + 2 años) y otra con `origen='P'`
      (`fecha_vigencia` vacía). Coinciden en todos los campos de negocio y
      difieren solo en `nro_acta`, `origen` y `fecha_vigencia`.

      Medición sobre la entrega 2025_2C (julio 2026):
        682.036 eventos con sola acta R
        135.714 pares R+P          -> 271.428 filas
          1.265 eventos solo P     (existen: NO se pueden descartar por origen)
             22 eventos con dos actas R
        = 819.037 eventos en 954.773 filas -> 14,2 % de exceso

      Inflación por año: 0,0 % hasta 2019 · 0,8 % en 2020 · 37,0 / 34,8 / 33,9 /
      37,1 / 36,6 % de 2021 a 2025. El quiebre cae sobre el corte de
      entrenamiento (2021_1C), y la duplicación NO es uniforme: las filas `P`
      son casi todas cursadas aprobadas (109.222 'Regular' + 27.579
      'Promocionó'), de modo que inflaba tanto los conteos como las tasas.

      La surrogate key anterior (`row_hash`) incluía `nro_acta` y `origen`, así
      que el par no era un duplicado exacto y atravesaba el filtro. La clave de
      deduplicación pasa a ser `evento_hash`, calculada sobre los campos que
      identifican el evento académico, sin los metadatos del acta.

      Criterio de retención: se prefiere la fila `origen='R'` porque conserva
      `fecha_vigencia`; cuando el evento existe solo como `P`, se retiene esa.
      Ningún evento se pierde. `n_actas` y `origenes` quedan como columnas de
      auditoría, y `row_hash` se conserva sobre la fila retenida.

  Las actas crudas siguen íntegras en la capa staging.
*/

with latest_tag as (
    select {{ get_latest_tag(ref('cursada_historica_flat')) }} as tag
),

src as (
    select *
    from {{ ref('cursada_historica_flat') }}
    where academic_period = (select tag from latest_tag)
),

-- Normalizamos legajo solo en canonical para alinear con alumnos/status
cleaned as (
    select
        academic_period,
        -- normalizamos legajo: trim, nos quedamos con la parte antes del punto, removemos no-dígitos
        case
            when regexp_replace(split_part(trim(legajo), '.', 1), '[^0-9]', '', 'g') ~ '^[0-9]+$'
            then regexp_replace(split_part(trim(legajo), '.', 1), '[^0-9]', '', 'g')
            else null
        end as legajo,
        cod_carrera,
        nom_carrera,
        anio,
        tipo_cursada,
        cod_materia,
        nom_materia,
        nro_acta,
        origen,
        nota,
        fecha,
        fecha_vigencia,
        resultado
    from src
),

-- Alcance institucional + claves de evento y de acta
keyed as (
    select
        -- clave del EVENTO académico: sin nro_acta, origen ni fecha_vigencia
        {{ dbt_utils.generate_surrogate_key([
            'legajo','cod_carrera','nom_carrera','anio','tipo_cursada','cod_materia',
            'nom_materia','nota','fecha','resultado'
        ]) }} as evento_hash,
        -- clave del ACTA: identifica el registro fuente (se conserva para auditoría)
        {{ dbt_utils.generate_surrogate_key([
            'legajo','cod_carrera','nom_carrera','anio','tipo_cursada','cod_materia',
            'nom_materia','nro_acta','origen','nota','fecha','fecha_vigencia','resultado'
        ]) }} as row_hash,
        academic_period,
        legajo,
        cod_carrera,
        nom_carrera,
        anio,
        tipo_cursada,
        cod_materia,
        nom_materia,
        nro_acta,
        origen,
        nota,
        fecha,
        fecha_vigencia,
        resultado
    from cleaned
    where legajo is not null
      and cod_carrera in {{ allowed_carreras() }}
),

-- Auditoría: cuántas actas distintas respaldan cada evento y con qué orígenes
actas_por_evento as (
    select
        evento_hash,
        count(distinct row_hash)                             as n_actas,
        string_agg(distinct coalesce(origen, '?'), '+' order by coalesce(origen, '?')) as origenes
    from keyed
    group by 1
)

-- Una fila por evento académico. Se prefiere la acta 'R' (conserva
-- fecha_vigencia); el desempate final por nro_acta hace el resultado
-- determinista ante reejecuciones del mismo snapshot.
select distinct on (k.evento_hash)
    k.evento_hash,
    k.row_hash,
    k.academic_period,
    k.legajo,
    k.cod_carrera,
    k.nom_carrera,
    k.anio,
    k.tipo_cursada,
    k.cod_materia,
    k.nom_materia,
    k.nro_acta,
    k.origen,
    a.n_actas,
    a.origenes,
    k.nota,
    k.fecha,
    k.fecha_vigencia,
    k.resultado,
    current_timestamp as inserted_at
from keyed k
join actas_por_evento a
  on a.evento_hash = k.evento_hash
order by k.evento_hash, k.origen desc nulls last, k.nro_acta
