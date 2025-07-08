{{ config(materialized='view') }}
select
    academic_period,
    payload->>'ID'                  as id,
    payload->>'COD_CARRERA'         as cod_carrera,
    payload->>'NOM_CARRERA'         as nom_carrera,
    payload->>'ANIO'                as anio,
    payload->>'TIPO_CURSADA'        as tipo_cursada,
    payload->>'COD_MATERIA'         as cod_materia,
    payload->>'NOM_MATERIA'         as nom_materia,
    payload->>'NRO_ACTA'            as nro_acta,
    payload->>'ORIGEN'              as origen,
    payload->>'NOTA'                as nota,
    payload->>'FECHA'               as fecha,
    payload->>'FECHA_VIGENCIA'      as fecha_vigencia,
    payload->>'RESULTADO'           as resultado
from {{ source('staging','cursada_historica_raw') }}