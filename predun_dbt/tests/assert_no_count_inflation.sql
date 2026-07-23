/*
  Control complementario al PSI: salto abrupto en la intensidad de cursado.

  MOTIVO: la duplicación de actas de 2021_1C subió la media de
  `materias_en_periodo` un 19 % entre el conjunto de referencia y el actual
  (3,510 -> 4,174) y el monitor de deriva NO la marcó: PSI = 0,076, nivel
  "ninguno". El PSI con bins por percentiles sub-detecta corrimientos en
  variables de conteo de baja cardinalidad, porque los bins de la referencia
  colapsan sobre pocos valores enteros y absorben el desplazamiento.

  Este test no reemplaza al PSI: lo cubre en su punto ciego, con una regla
  directa sobre la magnitud. La intensidad de cursado es una cantidad
  institucional estable (media ~3 materias por estudiante-período activo entre
  2011 y 2025 una vez deduplicado), así que un salto interperíodo grande es
  siempre un cambio de registración o un defecto de datos, no comportamiento.

  Umbral: 20 %, calibrado sobre la serie observada 2012-2025 (27 períodos con
  >= 500 estudiantes activos). Variación natural período a período: mediana
  1,9 %, p90 7,0 %, segundo salto más grande 13,1 % (2012_2C, cohorte todavía
  chica). El artefacto de duplicación produce 25,4 % en 2021_1C: queda
  holgadamente detectado y con margen sobre el ruido legítimo.

  Se compara solo entre períodos consecutivos con volumen suficiente
  (>= 500 estudiantes activos), para que los extremos de la serie no disparen
  falsos positivos.
*/

with medias as (
    select
        academic_period,
        count(*)                     as n_activos,
        avg(materias_en_periodo)     as media_materias
    from {{ ref('student_panel') }}
    where materias_en_periodo > 0
    group by 1
),

ordenadas as (
    select
        academic_period,
        n_activos,
        media_materias,
        lag(media_materias) over (order by academic_period) as media_previa,
        lag(n_activos)      over (order by academic_period) as n_previo
    from medias
)

select
    academic_period,
    media_previa,
    media_materias,
    round((media_materias / media_previa - 1) * 100, 1) as salto_pct
from ordenadas
where media_previa is not null
  and n_activos >= 500
  and n_previo  >= 500
  and abs(media_materias / media_previa - 1) > 0.20
