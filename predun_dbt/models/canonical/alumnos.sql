{{ config(
    materialized = 'table',
    full_refresh = true,
    pre_hook = ["{{ archive_canonical_table(this) }}"],
    post_hook = [
        'alter table {{ this }} add primary key (row_hash)'
    ]
) }}

with latest_tag as (
    select {{ get_latest_tag(ref('alumnos_flat')) }} as tag
),

src as (
    select *
    from {{ ref('alumnos_flat') }}
    where academic_period = (select tag from latest_tag)
),

-- Normalizamos legajo en canonical
cleaned as (
    select
        academic_period,
        case
            when regexp_replace(split_part(trim(legajo), '.', 1), '[^0-9]', '', 'g') ~ '^[0-9]+$'
            then regexp_replace(split_part(trim(legajo), '.', 1), '[^0-9]', '', 'g')
            else null
        end as legajo,
        calidad,
        nombre,
        plan_nombre,
        plan_codigo,
        anio_academico,
        fecha_inscripcion,
        regular,
        codigo_carrera,
        nombre_carrera,
        codigo_pertenece,
        nombre_pertenece,
        fecha_nacimiento,
        nacionalidad,
        pais_nacimiento,
        sexo,
        identidad_genero,
        tipo_ingreso,
        dpto_partido_nombre,
        localidad_nombre,
        pais_nombre
    from src
)

select distinct
    {{ dbt_utils.generate_surrogate_key([
        'legajo','calidad','nombre','plan_nombre','plan_codigo','anio_academico',
        'fecha_inscripcion','regular','codigo_carrera','nombre_carrera',
        'codigo_pertenece','nombre_pertenece','fecha_nacimiento','nacionalidad',
        'pais_nacimiento','sexo','identidad_genero','tipo_ingreso',
        'dpto_partido_nombre','localidad_nombre','pais_nombre'
    ]) }} as row_hash,
    academic_period,
    legajo,
    calidad,
    nombre,
    plan_nombre,
    plan_codigo,
    anio_academico,
    fecha_inscripcion,
    regular,
    codigo_carrera,
    nombre_carrera,
    codigo_pertenece,
    nombre_pertenece,
    fecha_nacimiento,
    nacionalidad,
    pais_nacimiento,
    sexo,
    identidad_genero,
    tipo_ingreso,
    dpto_partido_nombre,
    localidad_nombre,
    pais_nombre,
    current_timestamp as inserted_at
from cleaned
where legajo is not null