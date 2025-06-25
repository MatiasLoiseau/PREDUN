{{ config(
        materialized = 'incremental',
        unique_key   = 'row_hash',
        on_schema_change = 'sync_all_columns'
) }}

SELECT
    academic_period,
    id::TEXT,
    cod_carrera::TEXT,
    nom_carrera::TEXT,
    anio::INT,
    tipo_cursada::TEXT,
    cod_materia::TEXT,
    nom_materia::TEXT,
    nro_acta::TEXT,
    origen::TEXT,
    nota::NUMERIC,
    TO_DATE(fecha, 'YYYY-MM-DD')          AS fecha,
    TO_DATE(fecha_vigencia, 'YYYY-MM-DD') AS fecha_vigencia,
    UPPER(TRIM(resultado))                AS resultado,
    md5(concat_ws('|', id, cod_materia, academic_period)) AS row_hash
FROM {{ ref('staging_cursada_historica_flat') }}
WHERE academic_period = '2024_2C'

{% if is_incremental() %}
  AND row_hash NOT IN (SELECT row_hash FROM {{ this }})
{% endif %}