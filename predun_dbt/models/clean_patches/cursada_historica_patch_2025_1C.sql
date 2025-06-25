{{ config(
        materialized = 'incremental',
        unique_key   = 'row_hash',
        on_schema_change = 'sync_all_columns'
) }}

WITH src AS (
    SELECT *
    FROM {{ ref('staging_cursada_historica_flat') }}
    WHERE academic_period = '2025_1C'
)

SELECT
    academic_period,
    id::TEXT,
    cod_carrera::TEXT,
    nom_carrera::TEXT,
    anio::INT,
    tipo_cursada::TEXT,
    cod_materia::TEXT,
    -- ➊ Normaliza NOMBRE DE MATERIA quitando “(PLAN 20XX)” al final
    regexp_replace(
        UPPER(TRIM(nom_materia)),
        '\\s*\\(PLAN\\s+\\d{4}\\)$',
        '',
        'i'
    )                           AS nom_materia,
    nro_acta::TEXT,
    origen::TEXT,
    nota::NUMERIC,
    TO_DATE(fecha, 'YYYY-MM-DD')          AS fecha,
    TO_DATE(fecha_vigencia, 'YYYY-MM-DD') AS fecha_vigencia,
    -- ➋ Estandariza RESULTADO
    CASE
        WHEN lower(resultado) IN ('no promo', 'no_promociono', 'np')
             THEN 'NO PROMOCIONO'
        WHEN lower(resultado) IN ('promo', 'promociono')
             THEN 'PROMOCIONO'
        ELSE UPPER(TRIM(resultado))
    END                        AS resultado,
    md5(concat_ws('|', id, cod_materia, academic_period)) AS row_hash
FROM src

{% if is_incremental() %}
  AND row_hash NOT IN (SELECT row_hash FROM {{ this }})
{% endif %}