{{ config(
    materialized = 'table',
    full_refresh = true,
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
from src