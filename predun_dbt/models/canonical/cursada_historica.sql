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
from src