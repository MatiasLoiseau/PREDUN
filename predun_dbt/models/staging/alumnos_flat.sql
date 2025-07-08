{{ config(materialized='view') }}

select
    academic_period,
    payload->>'legajo'              as legajo,
    payload->>'calidad'             as calidad,
    payload->>'nombre'              as nombre,
    payload->>'plan_nombre'         as plan_nombre,
    payload->>'plan_codigo'         as plan_codigo,
    payload->>'anio_academico'      as anio_academico,
    payload->>'fecha_inscripcion'   as fecha_inscripcion,
    payload->>'regular'             as regular,
    payload->>'codigo_carrera'      as codigo_carrera,
    payload->>'nombre_carrera'      as nombre_carrera,
    payload->>'codigo_pertenece'    as codigo_pertenece,
    payload->>'nombre_pertenece'    as nombre_pertenece,
    payload->>'fecha_nacimiento'    as fecha_nacimiento,
    payload->>'nacionalidad'        as nacionalidad,
    payload->>'pais_nacimiento'     as pais_nacimiento,
    payload->>'sexo'                as sexo,
    payload->>'identidad_genero'    as identidad_genero,
    payload->>'tipo_ingreso'        as tipo_ingreso,
    payload->>'dpto_partido_nombre' as dpto_partido_nombre,
    payload->>'localidad_nombre'    as localidad_nombre,
    payload->>'pais_nombre'         as pais_nombre
from {{ source('staging','alumnos_raw') }}

