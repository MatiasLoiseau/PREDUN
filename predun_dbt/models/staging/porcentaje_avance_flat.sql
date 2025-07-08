{{ config(materialized='view') }}

select
    academic_period,
    payload->>'registro_id'        as registro_id,
    payload->>'persona_id'         as persona_id,
    payload->>'es_regular'         as es_regular,
    payload->>'orden_titulo'       as orden_titulo,
    payload->>'cod_carrera'        as cod_carrera,
    payload->>'nombre_carrera'     as nombre_carrera,
    payload->>'cod_titulo'         as cod_titulo,
    payload->>'titulo_obtenido'    as titulo_obtenido,
    payload->>'estado_titulo'      as estado_titulo,
    payload->>'reserva_1'          as reserva_1,
    payload->>'reserva_2'          as reserva_2,
    payload->>'vigente'            as vigente,
    payload->>'porcentaje_avance'  as porcentaje_avance,
    payload->>'materias_aprobadas' as materias_aprobadas
from {{ source('staging','porcentaje_avance_raw') }}