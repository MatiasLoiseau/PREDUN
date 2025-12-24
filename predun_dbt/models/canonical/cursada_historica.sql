{{ config(
    materialized = 'table',
    full_refresh = true,
    pre_hook = ["{{ archive_canonical_table(this) }}"],
    post_hook = [
        'alter table {{ this }} add primary key (row_hash)'
    ]
) }}

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
)

select distinct
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
    resultado,
    current_timestamp as inserted_at
from cleaned
where legajo is not null