{{ config(
    materialized = 'table',
    full_refresh = true,
    post_hook = [
        'alter table {{ this }} add primary key (row_hash)'
    ]
) }}

with latest_tag as (
    select {{ get_latest_tag(ref('porcentaje_avance_flat')) }} as tag
),

src as (
    select *
    from {{ ref('porcentaje_avance_flat') }}
    where academic_period = (select tag from latest_tag)
)

select distinct
    {{ dbt_utils.generate_surrogate_key([
        'registro_id','persona_id','es_regular','orden_titulo','cod_carrera',
        'nombre_carrera','cod_titulo','titulo_obtenido','estado_titulo',
        'reserva_1','reserva_2','vigente','porcentaje_avance','materias_aprobadas'
    ]) }} as row_hash,
    academic_period,
    registro_id,
    persona_id,
    es_regular,
    orden_titulo,
    cod_carrera,
    nombre_carrera,
    cod_titulo,
    titulo_obtenido,
    estado_titulo,
    reserva_1,
    reserva_2,
    vigente,
    porcentaje_avance,
    materias_aprobadas,
    current_timestamp as inserted_at
from src