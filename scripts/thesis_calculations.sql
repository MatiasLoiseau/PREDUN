-- =============================================================================
-- thesis_calculations.sql
-- Verificación de trazabilidad de las cifras de la tesis contra la base PREDUN.
--
-- Propósito: toda afirmación cuantitativa del texto debe poder reproducirse con
-- una consulta de este archivo. Cada bloque cita la frase textual de la tesis,
-- la consulta que la sostiene y el resultado verificado.
--
-- Ejecución:
--   docker exec -e PGPASSWORD=siu predun-postgres \
--     psql -U siu -d postgres -f /ruta/thesis_calculations.sql
--   (o pegar bloque por bloque en psql)
--
-- Snapshot verificado: entrega 2025_2C, datos hasta 2025-08-04.
-- Fecha de verificación: 2026-09-10
--
-- CONVENCIÓN DE VEREDICTOS
--   [OK]        la cifra del texto se reproduce exactamente
--   [CORREGIR]  la consulta devuelve un valor distinto al del texto
--   [MATIZAR]   la cifra es correcta pero la redacción afirma más de lo que sostiene
--   [CONTROL]   no está en el texto; verifica un supuesto que el texto usa
-- =============================================================================


-- #############################################################################
-- CAPÍTULO 1 — INTRODUCCIÓN
-- #############################################################################

-- -----------------------------------------------------------------------------
-- C1.1  [CORREGIR]  Total de legajos del registro académico
--
-- TEXTO: "Entre 2011 y 2025 su registro académico acumuló 75.600 legajos únicos"
--
-- El valor 75.600 solo aparece si se apilan las TRES entregas de staging y no se
-- normaliza el legajo. Es inconsistente con el resto del párrafo, que se calcula
-- sobre la entrega 2025_2C. Sobre ese mismo snapshot el total es 75.597.
-- Ver C1.1b: dos de los tres "legajos" de diferencia no son legajos.
-- -----------------------------------------------------------------------------
select
    academic_period,
    count(*)                       as filas,
    count(distinct trim(legajo))   as legajos_sin_normalizar,
    count(distinct regexp_replace(split_part(trim(legajo), '.', 1), '[^0-9]', '', 'g'))
                                   as legajos_normalizados
from staging.alumnos_flat
group by academic_period
order by academic_period;
-- 2024_2C  87.729  67.133  67.132
-- 2025_1C  97.236  73.559  73.559
-- 2025_2C  99.775  75.597  75.597   <-- CIFRA CORRECTA PARA EL TEXTO

select
    'apilando las 3 entregas (lo que da 75.600)' as scope,
    count(distinct trim(legajo))                as legajos_sin_normalizar,
    count(distinct regexp_replace(split_part(trim(legajo), '.', 1), '[^0-9]', '', 'g'))
                                                as legajos_normalizados
from staging.alumnos_flat;
-- 75.600 / 75.599


-- -----------------------------------------------------------------------------
-- C1.1b  [CONTROL]  Origen de la diferencia 75.600 vs 75.597
--
-- Los 3 legajos de diferencia son: un legajo real que existía en la entrega
-- 2024_2C y desapareció de 2025_2C, y DOS VALORES CORRUPTOS ('F' y 'M') que son
-- el campo sexo desplazado a la columna legajo en 14 filas de la entrega
-- 2024_2C. Se detectan porque su nombre_carrera trae códigos, no nombres.
-- La normalización de canonical.alumnos los elimina.
-- -----------------------------------------------------------------------------
with legajos_por_entrega as (
    select distinct academic_period, trim(legajo) as lg
    from staging.alumnos_flat
)
select lg, string_agg(academic_period, ',' order by academic_period) as entregas
from legajos_por_entrega
group by lg
having not bool_or(academic_period = '2025_2C')
order by lg;
-- 63557 | 2024_2C     (legajo real ausente en la entrega vigente)
-- F     | 2024_2C     (artefacto de parsing)
-- M     | 2024_2C     (artefacto de parsing)

-- Evidencia del desplazamiento de columnas: nombre_carrera contiene códigos
select academic_period, trim(legajo) as lg, count(*) as filas,
       string_agg(distinct coalesce(nombre_carrera, '(null)'), ' | ') as nombre_carrera
from staging.alumnos_flat
where trim(legajo) in ('F', 'M')
group by 1, 2
order by 2;
-- F | 8 filas | CA2 | CA7 | PT8 | SA2 | SA4 | SA5
-- M | 6 filas | AF1 | CA2 | CS2 | SA2 | SA5 | VOC01


-- -----------------------------------------------------------------------------
-- C1.2  [OK]  Rango temporal del registro
--
-- TEXTO: "Entre 2011 y 2025"
--
-- Se confirma por dos vías independientes: el padrón y las cursadas.
-- Nota: 2025 está incompleto — la entrega 2025_2C llega hasta 2025-08-04,
-- es decir, cubre hasta el 1C de 2025.
-- -----------------------------------------------------------------------------
select
    min(anio_academico)                     as anio_academico_min,
    max(anio_academico)                     as anio_academico_max,
    min(nullif(trim(fecha_inscripcion),'')) as inscripcion_min,
    max(nullif(trim(fecha_inscripcion),'')) as inscripcion_max
from staging.alumnos_flat
where academic_period = '2025_2C';
-- 2011 | 2025 | 2011-01-20 | 2025-08-04

select
    min(trim(fecha)::date) as primera_cursada,
    max(trim(fecha)::date) as ultima_cursada
from canonical.cursada_historica
where trim(coalesce(fecha, '')) ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$';
-- 2011-07-08 | 2025-08-04


-- -----------------------------------------------------------------------------
-- C1.3  [MATIZAR]  Composición de la oferta académica
--
-- TEXTO: "corresponden a estudiantes de carreras de grado, tecnicaturas,
--         posgrados y cursos de extensión"
--
-- La enumeración omite una categoría con peso propio: los 10 CICLOS DE
-- COMPLEMENTACIÓN CURRICULAR (3.520 legajos), que son carreras de GRADO y están
-- excluidos del estudio. Además, lo que el texto llama "cursos de extensión"
-- son en los datos talleres, cursos y diplomaturas.
-- (Un legajo puede aparecer en más de una categoría: los totales no suman.)
-- -----------------------------------------------------------------------------
with clasificado as (
    select distinct
        legajo,
        codigo_carrera,
        case
            when codigo_carrera in (
                    'SA5','AB5','AF3','PT2','PT7','AF1','CA2','AB1','TA06','PT6',
                    'TA1','SA2','PT8','CA1','CS2','AF0','CA4','PT1','SA1','CS0',
                    'CS3','PT3','CS1','CS9','CS4','AB4','PT9','SA4','PT4'
                 )
                 then '1. Grado/Tecnicatura (las 29 del estudio)'
            when nombre_carrera ilike '%Ciclo de Complementaci%'
                 then '2. Ciclo de Complementacion Curricular (GRADO, excluido)'
            when nombre_carrera ilike '%Maestr%'
              or nombre_carrera ilike '%Doctorado%'
              or nombre_carrera ilike '%Especializaci%'
              or nombre_carrera ilike '%Posgrado%'
                 then '3. Posgrado'
            when nombre_carrera ilike '%Taller%'
              or nombre_carrera ilike '%Curso%'
              or nombre_carrera ilike '%Diplomatura%'
              or nombre_carrera ilike '%Programa de Formacion%'
                 then '4. Taller/Curso/Diplomatura'
            else '5. Otros (movilidad estudiantil)'
        end as tipo
    from staging.alumnos_flat
    where academic_period = '2025_2C'
)
select tipo, count(distinct codigo_carrera) as carreras, count(distinct legajo) as legajos
from clasificado
group by tipo
order by tipo;
-- 1. Grado/Tecnicatura (las 29 del estudio)                  29    69.779
-- 2. Ciclo de Complementacion Curricular (GRADO, excluido)   10     3.520
-- 3. Posgrado                                                19     1.961
-- 4. Taller/Curso/Diplomatura                                18     8.116
-- 5. Otros (movilidad estudiantil)                            2       771
--
-- La lista de 29 códigos está inlineada aquí para que el archivo sea ejecutable.
-- Fuente de verdad: predun_dbt/macros/allowed_carreras.sql — si cambia allí,
-- actualizar aquí.


-- -----------------------------------------------------------------------------
-- C1.4  [OK]  Legajos con actividad de cursada y padrón total
--
-- TEXTO: "los 55.335 legajos con actividad de cursada"
--
-- canonical.* ya está filtrado a la entrega vigente y a las 29 carreras.
-- -----------------------------------------------------------------------------
select
    (select count(distinct legajo) from canonical.alumnos)           as padron_29_carreras,
    (select count(distinct legajo) from canonical.cursada_historica) as legajos_con_cursada,
    (select count(*)              from marts.student_status)         as filas_student_status;
-- 69.779 | 55.335 | 69.779


-- -----------------------------------------------------------------------------
-- C1.5  [MATIZAR]  Las 29 carreras
--
-- TEXTO: "las 29 carreras de grado y tecnicatura que la universidad dictó
--         durante ese período"
--
-- Dos imprecisiones:
--   (a) NO son todas las carreras de grado: los Ciclos de Complementación
--       Curricular también otorgan título de grado y quedan fuera (ver C1.3).
--   (b) NO se dictaron "durante ese período" entendido como todo el período:
--       4 dejaron de registrar actividad antes de 2025 (PT3, CS4, SA1, CS1) y
--       varias comenzaron mucho después de 2011 (TA06 recién en 2022).
-- El conteo de 29 sí es exacto.
-- -----------------------------------------------------------------------------
select count(distinct cod_carrera) as carreras_en_canonical
from canonical.cursada_historica;
-- 29

select
    cod_carrera,
    min(extract(year from trim(fecha)::date))::int as anio_inicio,
    max(extract(year from trim(fecha)::date))::int as anio_fin,
    count(distinct legajo)                          as legajos
from canonical.cursada_historica
where trim(coalesce(fecha, '')) ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
group by cod_carrera
order by anio_fin, anio_inicio;
-- PT3  2013-2018 |  CS4  2012-2019 |  SA1  2011-2021 |  CS1  2011-2024
-- TA06 2022-2025 (la más reciente)

-- Carreras de grado excluidas del estudio (Ciclos de Complementación) con
-- actividad de cursada real:
select cod_carrera, min(nom_carrera) as nom_carrera, count(distinct legajo) as legajos
from staging.cursada_historica_flat
where academic_period = '2025_2C'
  and nom_carrera ilike '%Ciclo de Complementaci%'
group by cod_carrera
order by legajos desc;
-- CS6 651 | CA3 620 | SA3 558 | AF2 471 | CSB 317 | TA05 156 | CA7 137
-- CA08 133 | AF4 65 | PT5 34


-- -----------------------------------------------------------------------------
-- C1.6  [OK]  Distribución de estados sobre los legajos con cursada
--
-- TEXTO: "el 56,1% de los 55.335 legajos con actividad de cursada figura en
--         estado de abandono, el 36,0% cursa de forma activa y el 7,9% alcanzó
--         la finalización curricular estimada"
--
-- Reproduce exacto. Lógica en predun_dbt/models/marts/student_status.sql
-- -----------------------------------------------------------------------------
with con_cursada as (
    select s.status
    from marts.student_status s
    where exists (
        select 1 from canonical.cursada_historica c where c.legajo = s.legajo
    )
)
select
    status,
    count(*)                                            as legajos,
    round(100.0 * count(*) / sum(count(*)) over (), 3)  as pct
from con_cursada
group by status
order by legajos desc;
-- abandonó               31.044   56,102  -> 56,1 %
-- estudiando             19.927   36,012  -> 36,0 %
-- finalizacion_estimada   4.364    7,887  ->  7,9 %
-- total                  55.335


-- -----------------------------------------------------------------------------
-- C1.7  [MATIZAR]  Inscriptos que nunca cursaron
--
-- TEXTO: "los 14.444 inscriptos que nunca llegaron a cursar una materia"
--
-- El 14.444 es correcto PARA LAS 29 CARRERAS DEL ESTUDIO. Pero 142 de ellos sí
-- cursaron materias, en carreras fuera del recorte (ciclos de complementación,
-- talleres, posgrados). Los que nunca cursaron NADA en la universidad son 14.302.
-- La afirmación literal ("nunca llegaron a cursar una materia") sobreestima en 142.
-- -----------------------------------------------------------------------------
with cursada_toda_la_universidad as (
    select distinct regexp_replace(split_part(trim(legajo), '.', 1), '[^0-9]', '', 'g') as legajo
    from staging.cursada_historica_flat
    where academic_period = '2025_2C'
),
nunca_en_las_29 as (
    select s.legajo
    from marts.student_status s
    where not exists (
        select 1 from canonical.cursada_historica c where c.legajo = s.legajo
    )
)
select
    (select count(*) from nunca_en_las_29) as nunca_cursaron_en_las_29,
    (select count(*) from nunca_en_las_29 n
       where exists (select 1 from cursada_toda_la_universidad x where x.legajo = n.legajo))
                                           as si_cursaron_fuera_de_las_29,
    (select count(*) from nunca_en_las_29 n
       where not exists (select 1 from cursada_toda_la_universidad x where x.legajo = n.legajo))
                                           as nunca_cursaron_nada;
-- 14.444 | 142 | 14.302


-- -----------------------------------------------------------------------------
-- C1.8  [CORREGIR]  Tasa de abandono incluyendo a los que nunca cursaron
--
-- TEXTO: "la tasa de abandono llega a casi el 65%"
--
-- Son 65,19 %, que SUPERA el 65 %, no "casi". El adverbio invierte el sentido
-- del redondeo y subestima la cifra propia del trabajo.
-- Sugerido: "supera el 65 %" o directamente "65,2 %".
-- -----------------------------------------------------------------------------
select
    count(*) filter (where dropout_flag = 1)                              as en_abandono,
    count(*)                                                              as padron_total,
    round(100.0 * count(*) filter (where dropout_flag = 1) / count(*), 3) as pct_abandono
from marts.student_status;
-- 45.488 | 69.779 | 65,189


-- -----------------------------------------------------------------------------
-- C1.9  [CONTROL]  ¿El abandono está inflado por ausencia del dato de avance?
--
-- student_status.sql hace coalesce(porcentaje_avance, 0): un legajo sin dato de
-- avance NUNCA puede clasificarse como finalización estimada y cae en abandono.
-- Si la cobertura del dato fuera baja, el 56,1 % sería en parte un artefacto.
--
-- RESULTADO: la cobertura es prácticamente total. Solo 6 legajos del padrón no
-- tienen dato de avance, de los cuales 2 quedan en abandono y 1 tiene cursada.
-- El riesgo queda descartado. Vale la pena citar este control en el texto.
-- -----------------------------------------------------------------------------
select
    (select count(distinct legajo) from canonical.porcentaje_avance) as legajos_con_dato_avance,
    (select count(*) from marts.student_status s
       where not exists (select 1 from canonical.porcentaje_avance p where p.legajo = s.legajo))
                                                                     as padron_sin_dato,
    (select count(*) from marts.student_status s
       where s.status = 'abandonó'
         and not exists (select 1 from canonical.porcentaje_avance p where p.legajo = s.legajo))
                                                                     as abandono_sin_dato,
    (select count(*) from marts.student_status s
       where s.status = 'abandonó'
         and exists (select 1 from canonical.cursada_historica c where c.legajo = s.legajo)
         and not exists (select 1 from canonical.porcentaje_avance p where p.legajo = s.legajo))
                                                                     as abandono_con_cursada_sin_dato;
-- 69.787 | 6 | 2 | 1


-- -----------------------------------------------------------------------------
-- C1.10  [CONTROL]  Sensibilidad del umbral de 90 % de avance
--
-- El texto presenta el 7,9 % de finalización estimada sin indicar cuánto depende
-- del umbral elegido. Con 100 % la cifra cae a la mitad (4,4 %); con 80 % sube a
-- 9,3 %. La sensibilidad es moderada y el umbral está justificado en el Cap. 4
-- (optativas + tesis pendiente), pero conviene tener el número a mano en defensa.
-- -----------------------------------------------------------------------------
with avance as (
    select legajo,
           max(case when trim(porcentaje_avance) ~ '^[0-9]+([.,][0-9]+)?$'
                    then replace(trim(porcentaje_avance), ',', '.')::numeric end) as pct
    from canonical.porcentaje_avance
    group by legajo
),
con_cursada as (select distinct legajo from canonical.cursada_historica)
select
    u.umbral,
    count(*) filter (where avance.pct >= u.umbral)                        as n_finalizacion,
    round(100.0 * count(*) filter (where avance.pct >= u.umbral) / 55335.0, 2) as pct_sobre_55335
from avance
join con_cursada using (legajo),
     (values (80), (85), (90), (95), (100)) as u(umbral)
group by u.umbral
order by u.umbral;
--  80   5.139   9,29 %
--  85   4.767   8,61 %
--  90   4.364   7,89 %   <-- el del texto
--  95   3.761   6,80 %
-- 100   2.433   4,40 %


-- -----------------------------------------------------------------------------
-- C1.11  [CONTROL]  Sensibilidad de la ventana de inactividad de 2 años
--
-- ESTE ES EL CONTROL MÁS IMPORTANTE DEL CAPÍTULO. El 56,1 % que abre la tesis
-- depende críticamente de la ventana elegida: va de 65,0 % (1 año) a 37,0 %
-- (4 años). Casi 30 puntos porcentuales de rango.
--
-- La elección de 2 años está justificada en el Cap. 4 por la caída de la tasa de
-- reactivación (ver predictions.sensitivity_horizon), pero el Cap. 1 presenta el
-- 56,1 % como un hecho sin señalar que es una cifra dependiente de una decisión
-- operativa. Es la primera pregunta que hará el jurado.
-- -----------------------------------------------------------------------------
with params as (
    select max(trim(fecha)::date) as data_max_date
    from canonical.cursada_historica
    where trim(coalesce(fecha, '')) ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
),
ultima_cursada as (
    select legajo, max(fecha)::date as fecha_ultima
    from canonical.cursada_historica
    group by legajo
),
finalizacion as (
    select distinct legajo
    from canonical.porcentaje_avance
    where coalesce(case when trim(porcentaje_avance) ~ '^[0-9]+([.,][0-9]+)?$'
                        then replace(trim(porcentaje_avance), ',', '.')::numeric end, 0) >= 90
)
select
    v.anios,
    count(*) filter (
        where finalizacion.legajo is null
          and u.fecha_ultima < p.data_max_date - (v.anios || ' years')::interval
    ) as n_abandono,
    round(100.0 * count(*) filter (
        where finalizacion.legajo is null
          and u.fecha_ultima < p.data_max_date - (v.anios || ' years')::interval
    ) / 55335.0, 2) as pct_sobre_55335
from ultima_cursada u
cross join params p
left join finalizacion using (legajo),
     (values (1), (2), (3), (4)) as v(anios)
group by v.anios
order by v.anios;
-- 1 año    35.978   65,02 %
-- 2 años   31.044   56,10 %   <-- el del texto
-- 3 años   26.181   47,31 %
-- 4 años   20.486   37,02 %

-- Contraste con el análisis que ya justifica el horizonte (Cap. 4):
select * from predictions.sensitivity_horizon order by horizonte_periodos;
-- horizonte 4 períodos (2 años) -> tasa de reactivación 8,43 %


-- -----------------------------------------------------------------------------
-- C1.12  [CONTROL]  Volumen histórico procesado
--
-- Respalda el objetivo específico 4: "que el sistema pueda procesar el volumen
-- histórico de la institución".
-- -----------------------------------------------------------------------------
select 'staging.cursada_historica_flat' as tabla, count(*) as filas from staging.cursada_historica_flat
union all select 'staging.alumnos_flat',           count(*) from staging.alumnos_flat
union all select 'staging.porcentaje_avance_flat', count(*) from staging.porcentaje_avance_flat
union all select 'canonical.cursada_historica',    count(*) from canonical.cursada_historica
union all select 'marts.student_panel',            count(*) from marts.student_panel
union all select 'marts.student_status',           count(*) from marts.student_status
order by filas desc;
-- staging.cursada_historica_flat  2.879.076   (3 entregas apiladas)
-- canonical.cursada_historica       819.037   (entrega vigente, deduplicada)
-- marts.student_panel               687.186
-- staging.alumnos_flat              284.740
-- staging.porcentaje_avance_flat    268.131
-- marts.student_status               69.779
-- Total staging: 3.431.947 filas


-- -----------------------------------------------------------------------------
-- C1.13  [CONTROL]  Ciclos cuatrimestrales sucesivos efectivamente ejecutados
--
-- Respalda el objetivo específico 6: "a lo largo de ciclos cuatrimestrales
-- sucesivos". Tres ciclos con entrenamiento, evaluación y selección registrados.
-- -----------------------------------------------------------------------------
select cycle_period, model_name, round(roc_auc::numeric, 4) as roc_auc,
       is_selected_model, evaluated_at
from predictions.model_evaluations
order by cycle_period, roc_auc desc;
-- 2024_2C / 2025_1C / 2025_2C, 3 modelos por ciclo, 1 seleccionado por ciclo

-- Configuración declarativa por entrega (objetivo específico 1 e hipótesis 1):
-- 9 archivos YAML en predun_dagster/ingestion/mappings/fix_and_clean/
--   v{2024_2C,2025_1C,2025_2C}.yaml            (cursada histórica)
--   students_v{...}.yaml                        (alumnos)
--   percentage_v{...}.yaml                      (porcentaje de avance)
-- Una entrega nueva de una fuente existente se incorpora agregando un YAML.


-- #############################################################################
-- CAPÍTULO 2 — ESTADO DEL ARTE
-- #############################################################################
--
-- Capítulo mayormente bibliográfico. La verificación se hizo por dos vías.
--
-- (A) CONTRA FUENTE PRIMARIA (los PDF de bibliografia/usados-en-tesis/).
--     No genera SQL. Hallazgos, para dejar constancia:
--
--     [CORREGIR] "la SPU informaba entre 19 y 22 graduados por cada 100
--       inscriptos, tomando el tiempo normal de duración de la carrera"
--       -> garciafanelli2014 p.11-12: el 19 es de la SPU pero sobre "20 carreras
--          seleccionadas"; el 22 NO es de la SPU sino un indicador alternativo
--          que construye la propia autora (García de Fanelli, 2011) como cociente
--          egresados 2006-2009 / inscriptos 2001-2003. La fuente dice literal que
--          "la SPU no ha continuado midiendo este indicador". El "tiempo normal
--          de duración" aplica solo al 19.
--
--     [CORREGIR] "En disciplinas como ingeniería ese valor bajaba al 13%"
--       -> garciafanelli2014 p.12-13: el 13 % proviene de las acreditaciones de
--          CONEAU (no de la SPU) y corresponde a cohortes 1988-1998 (no al
--          período 2002-2012 que revisa el artículo). No es "ese valor"
--          desagregado por disciplina, es otro indicador de otra fuente.
--
--     [CORREGIR] "la idea de primera deserción de Tinto [tinto1993]"
--       -> Tesis_MPustilnik.pdf: "la definición de 'primera deserción'
--          proporcionada por Tinto en 1982". Su bibliografía lista Tinto (1982),
--          "Limits of theory and practice in student attrition", J. of Higher
--          Education 53(6), 687-700. Tinto 1993 también está en su bibliografía
--          pero NO es la que sustenta ese concepto.
--
--     [CORREGIR] "el riesgo se puede anticipar desde la admisión, ya que el
--       primer año concentra la mayor parte del fenómeno"
--       -> bonifro2020 no afirma la cláusula causal. Justifica la predicción
--          temprana por la oportunidad de intervención, y dice lo contrario
--          sobre la dificultad ("increases the difficulty of the task").
--          La afirmación SÍ es cierta para UNDAV -> ver C2.1, que la respalda
--          con dato propio en lugar de con una cita que no la sostiene.
--
--     [MATIZAR] "2,24 veces mayor" es un odds ratio. garciafanelli2015 p.24 dice
--       "se ha optado por presentar la razón de probabilidades u odds ratio" y
--       recién después lo verbaliza como "probabilidad 2,24 veces más grande".
--       La tesis es fiel a la fuente, pero un jurado de ciencia de datos
--       distingue odds ratio de razón de riesgos.
--
--     [MATIZAR] "decenas de miles de estudiantes" -> herodotou2020 da la cifra
--       exacta: 1.182 docentes y 23.640 estudiantes en 231 cursos (abstract;
--       el cuerpo dice 1.159 y 23.180). Además "reached" son los estudiantes
--       alcanzados por el sistema, no los identificados en riesgo.
--
--     [MATIZAR] edm2024 (única cita que sostiene "los trabajos realizados en
--       universidades públicas latinoamericanas"): es UN estudio, n=329
--       (109 desertores + 220 no), Universidad Nacional de Moquegua, Perú,
--       reporta accuracy (76 % máx.) y no AUC, y está publicado en
--       "Nanotechnology Perceptions", una revista de nanotecnología.
--       Sus variables influyentes son socioeconómicas y de vivienda (ingreso,
--       carga familiar, tipo de construcción, nº de dormitorios), NO de
--       trayectoria académica -> contradice la primera de las tres coincidencias
--       que el capítulo enuncia ("son SIEMPRE las más predictivas").
--
--     [MATIZAR] insidehighered2013 es una nota periodística sosteniendo la
--       crítica metodológica central a Course Signals.
--
--     Verificadas correctas: Pustilnik AUC 0,88 con XGBoost (Árboles 0,84,
--     SVM 0,74, Tabla 19); SIU-Guaraní; geocodificación con Google Maps y
--     tiempo de viaje en transporte público; becas y horas de trabajo;
--     "no rinde ninguna evaluación en un semestre"; unidad = persona sumando
--     todas las carreras; recomendación de reentrenar cada semestre; 37,7 %;
--     50 %; mayor abandono en varones; período 2002-2012; Kim et al =
--     Gyeongsang National University (Corea del Sur), grupo "High-Risk",
--     precision/recall; OU Analyse 4 años; causalidad inversa en Course
--     Signals; y las 19 claves de cita existen en bibliography.bib.
--
-- (B) CONTRA LA BASE DE PREDUN. Es lo que sigue.


-- -----------------------------------------------------------------------------
-- C2.1  [CONTROL]  ¿El primer año concentra la mayor parte del abandono?
--
-- TEXTO: "el riesgo se puede anticipar desde la admisión, ya que el primer año
--         concentra la mayor parte del fenómeno" (atribuido a bonifro2020)
--
-- La fuente citada no dice eso, pero el dato propio SÍ lo sostiene y con holgura.
-- Conviene reemplazar la cita prestada por esta evidencia de UNDAV.
--
-- Se mide la permanencia como la distancia entre la primera y la última cursada
-- registrada del legajo.
-- -----------------------------------------------------------------------------
with primera as (
    select legajo, min(trim(fecha)::date) as f_ini
    from canonical.cursada_historica
    where trim(coalesce(fecha, '')) ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
    group by legajo
),
ultima as (
    select legajo, max(trim(fecha)::date) as f_fin
    from canonical.cursada_historica
    where trim(coalesce(fecha, '')) ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
    group by legajo
),
abandono as (select legajo from marts.student_status where status = 'abandonó'),
permanencia as (
    select least(floor((u.f_fin - p.f_ini) / 365.25)::int, 5) as anios
    from abandono a
    join primera p on p.legajo = a.legajo
    join ultima  u on u.legajo = a.legajo
)
select
    anios as anios_de_permanencia,
    count(*) as legajos_en_abandono,
    round(100.0 * count(*) / sum(count(*)) over (), 2) as pct,
    round(100.0 * sum(count(*)) over (order by anios) / sum(count(*)) over (), 2) as pct_acumulado
from permanencia
group by anios
order by anios;
-- 0 años (< 1 año)  20.050   64,59 %   64,59 %  <-- el primer año concentra
-- 1 año              4.142   13,34 %   77,93 %
-- 2 años             2.281    7,35 %   85,28 %
-- 3 años             1.636    5,27 %   90,55 %
-- 4 años             1.044    3,36 %   93,91 %
-- 5 o más            1.891    6,09 %  100,00 %

-- Incluyendo a los que nunca llegaron a cursar (abandono antes de empezar):
with primera as (
    select legajo, min(trim(fecha)::date) as f_ini
    from canonical.cursada_historica
    where trim(coalesce(fecha, '')) ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
    group by legajo
),
ultima as (
    select legajo, max(trim(fecha)::date) as f_fin
    from canonical.cursada_historica
    where trim(coalesce(fecha, '')) ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
    group by legajo
),
abandono as (select legajo from marts.student_status where status = 'abandonó')
select
    count(*)                                                   as abandono_total,
    count(*) filter (where p.legajo is null)                   as nunca_cursaron,
    count(*) filter (where p.legajo is not null
                       and (u.f_fin - p.f_ini) < 365.25)       as abandono_en_primer_anio,
    round(100.0 * count(*) filter (where p.legajo is null
                       or (u.f_fin - p.f_ini) < 365.25) / count(*), 2)
                                                               as pct_primer_anio_o_antes
from abandono a
left join primera p on p.legajo = a.legajo
left join ultima  u on u.legajo = a.legajo;
-- 45.488 | 14.444 | 20.050 | 75,83 %


-- -----------------------------------------------------------------------------
-- C2.2  [CORREGIR]  El desbalance de clases NO aplica a este caso
--
-- TEXTO: "El desbalance de clases, donde el abandono es la clase minoritaria,
--         es el principal desafío técnico."
--
-- Es cierto para la literatura citada (bonifro2020 reporta abandono < 12,3 %,
-- ratio 1:7), pero NO para UNDAV. En la población de modelado la prevalencia es
-- del 38,3 %, y sobre el panel completo el abandono es la clase MAYORITARIA.
-- El capítulo enuncia como desafío general algo que en su propio caso no se
-- presenta. Conviene decirlo, porque además es una ventaja del diseño.
-- -----------------------------------------------------------------------------
select
    'panel completo' as poblacion,
    count(*) filter (where dropout_next is not null)  as etiquetadas,
    count(*) filter (where dropout_next = 1)          as positivas,
    count(*) filter (where dropout_next = 0)          as negativas,
    round(100.0 * count(*) filter (where dropout_next = 1)
          / nullif(count(*) filter (where dropout_next is not null), 0), 2) as prevalencia_pct
from marts.student_panel
union all
select
    'población de modelado (at_risk = 1)',
    count(*) filter (where dropout_next is not null),
    count(*) filter (where dropout_next = 1),
    count(*) filter (where dropout_next = 0),
    round(100.0 * count(*) filter (where dropout_next = 1)
          / nullif(count(*) filter (where dropout_next is not null), 0), 2)
from marts.student_panel
where at_risk = 1;
-- panel completo                        479.660  277.465  202.195   57,85 %
-- población de modelado (at_risk = 1)   304.175  116.368  187.807   38,26 %
--
-- Contraste con la literatura citada en el capítulo:
--   bonifro2020  -> abandono < 12,3 % (ratio 1:7), desbalance severo
--   edm2024      -> 109 de 329 (33 %)
--   UNDAV        -> 38,3 % en modelado, sin desbalance severo


-- -----------------------------------------------------------------------------
-- C2.3  [CORREGIR]  La equivalencia UNDAV/UNAHUR no es verificable
--
-- TEXTO: "una alta proporción de estudiantes de primera generación. El perfil
--         institucional es prácticamente igual al de UNDAV."
--
-- Pustilnik documenta el dato PARA UNAHUR (77 % de una cohorte, 84 % de los
-- ingresantes 2020). Del lado de UNDAV el dato NO EXISTE en PREDUN. No hay
-- ninguna variable de primera generación, nivel educativo del hogar ni
-- condición socioeconómica. La equivalencia se afirma sin evidencia propia.
-- -----------------------------------------------------------------------------
select column_name, data_type
from information_schema.columns
where table_schema = 'canonical' and table_name = 'alumnos'
order by ordinal_position;
-- 24 columnas. Las únicas de contexto personal son fecha_nacimiento,
-- nacionalidad, pais_nacimiento, sexo, identidad_genero, tipo_ingreso,
-- dpto_partido_nombre, localidad_nombre, pais_nombre.
-- Ninguna informa primera generación ni nivel socioeconómico.

-- Confirmación de que no existe tal columna en ninguna tabla de la base:
select table_schema, table_name, column_name
from information_schema.columns
where table_schema in ('staging', 'canonical', 'marts')
  and (column_name ~* 'generacion|generation|educacion_padre|nivel_educativo|ingreso_familiar|socioecon');
-- 0 filas


-- -----------------------------------------------------------------------------
-- C2.4  [CONTROL]  Los formatos de datos sí cambian de un ciclo a otro
--
-- TEXTO (cierre del capítulo): "en una universidad cuyas cohortes, planes de
--        estudio y formatos de datos cambian de un ciclo lectivo a otro"
--
-- La afirmación es verdadera y se puede demostrar con datos propios, pero el
-- capítulo la usa como cierre retórico sin cuantificarla. La evidencia es fuerte
-- y conviene aprovecharla, porque además respalda la hipótesis 1 del Capítulo 1.
--
-- (a) EN EL ORIGEN — los 9 YAML de predun_dagster/ingestion/mappings/fix_and_clean/
--     muestran que las tres fuentes cambiaron de formato en las tres entregas:
--
--     Fuente     Entrega   Columnas  Delimitador  Encoding
--     cursada    2024_2C      13          |       ISO-8859-15
--     cursada    2025_1C      18          |       utf-8
--     cursada    2025_2C      18          ,       utf-8
--     alumnos    2024_2C      16          ,       utf-8
--     alumnos    2025_1C      38          ,       ISO-8859-1
--     alumnos    2025_2C      38          ,       utf-8
--     avance     2024_2C      12          ,       utf-8
--     avance     2025_1C      18          ,       utf-8
--     avance     2025_2C      14          ,       utf-8
--
--     Cambian las tres dimensiones a la vez y ninguna fuente es estable.
--     El avance oscila (12 -> 18 -> 14), no crece de forma monótona.
--
-- (b) EN STAGING — la configuración declarativa absorbe la heterogeneidad de
--     FORMATO (delimitador y encoding) sin tocar código. Lo que sí varía es el
--     conjunto de campos que trae cada entrega, y eso se resuelve recién en
--     canonical, que impone el esquema (schema-on-read).
-- -----------------------------------------------------------------------------
select 'cursada' as fuente, academic_period, count(distinct k) as claves_en_payload
from staging.cursada_historica_raw, lateral jsonb_object_keys(payload::jsonb) k
group by 1, 2
union all
select 'alumnos', academic_period, count(distinct k)
from staging.alumnos_raw, lateral jsonb_object_keys(payload::jsonb) k
group by 1, 2
union all
select 'avance', academic_period, count(distinct k)
from staging.porcentaje_avance_raw, lateral jsonb_object_keys(payload::jsonb) k
group by 1, 2
order by 1, 2;
-- alumnos 2024_2C 12 | 2025_1C 21 | 2025_2C 21
-- avance  2024_2C 12 | 2025_1C 14 | 2025_2C 14
-- cursada 2024_2C 13 | 2025_1C 13 | 2025_2C 13   <-- formato distinto en origen,
--                                                    staging uniforme


-- -----------------------------------------------------------------------------
-- C2.5  [CONTROL]  Los planes de estudio también cambian
--
-- Respalda la misma frase de cierre. 70 planes distintos para 29 carreras.
-- -----------------------------------------------------------------------------
select
    count(distinct plan_codigo)     as planes_distintos,
    count(distinct codigo_carrera)  as carreras,
    round(count(distinct plan_codigo)::numeric / count(distinct codigo_carrera), 2)
                                    as planes_por_carrera
from canonical.alumnos;
-- 70 | 29 | 2,41

select codigo_carrera, count(distinct plan_codigo) as planes
from canonical.alumnos
group by codigo_carrera
having count(distinct plan_codigo) > 1
order by planes desc;
-- CA2 5 | AF1 4 | PT2 4 | SA2 4 | AB1 4 | AF0 3 | CA1 3 | AF3 3 | ...


-- -----------------------------------------------------------------------------
-- C2.6  [MATIZAR]  "Contratos de datos" — qué hay realmente en PREDUN
--
-- TEXTO: "Ninguno de los trabajos relevados documenta una plataforma de MLOps
--         que cubra el ciclo completo, es decir la ingesta con contratos de
--         datos, [...] y el monitoreo continuo."
--
-- Dos problemas en la frase que DEFINE el vacío que la tesis promete cubrir.
--
-- (1) PREDUN no implementa contratos de datos en sentido técnico. No hay
--     ninguna cláusula `contract:` en los modelos dbt. Lo que sí hay es
--     validación de esquema en la ingesta (`expected_columns` en los YAML,
--     que descarta filas con un número de campos distinto al esperado) y 32
--     tests declarados en los schema.yml de canonical y marts. Es defendible
--     llamarlo "validación de esquema en la ingesta", no "contratos de datos".
--
-- (2) "monitoreo continuo" quedó sin moderar. El Capítulo 1 dice "monitoreo
--     automatizado en cada ciclo de actualización". Es la única aparición de
--     "monitoreo continuo" en toda la tesis, y contradice esa definición.
--     (Ojo: "entrenamiento continuo" en los caps. 3, 5 y 7 es otra cosa —
--     es el continuous training del modelo de madurez de Google — y está bien.)
--
-- Verificación de lo que sí existe:
-- -----------------------------------------------------------------------------
select
    (select count(*) from marts.student_panel  where legajo is null)          as panel_legajo_nulo,
    (select count(*) from marts.student_status where legajo is null)          as status_legajo_nulo,
    (select count(distinct legajo) from marts.student_status)                 as status_legajos_unicos,
    (select count(*) from marts.student_status)                               as status_filas;
-- 0 | 0 | 69.779 | 69.779  -> unicidad y no-nulidad efectivas
--
-- Los tests declarativos viven en predun_dbt/models/{canonical,marts}/schema.yml
-- (32 en total, incluido unique_combination_of_columns) y se ejecutan con
-- `dbt test`. Junto con tests/assert_no_count_inflation.sql son la garantía
-- real de calidad, y son lo que el texto debería nombrar.


-- #############################################################################
-- CAPÍTULO 3 — MARCO TEÓRICO
-- #############################################################################
--
-- Capítulo conceptual. Se verificó por tres vías.
--
-- (A) CONTRA FUENTE PRIMARIA. Hallazgos que no generan SQL:
--
--     [CORREGIR — GRAVE] "Google propone un modelo de madurez de tres niveles"
--       El PDF de `googleMLOpsWhitepaper` es "Practitioners guide to MLOps"
--       (Salama, Kazmierczak y Schut, mayo 2021, 37 págs). En sus 37 páginas hay
--       CERO apariciones de "level 0", "level 1", "level 2", "MLOps level",
--       "manual process" y "pipeline automation". Ese documento define un
--       conjunto de "core MLOps capabilities", no un modelo de madurez.
--       Los niveles 0/1/2 son del artículo del Google Cloud Architecture Center
--       "MLOps: Continuous delivery and automation pipelines in machine
--       learning", que es OTRO documento y no está en la carpeta de bibliografía.
--       Además la entrada del .bib FUSIONA los títulos de los dos documentos,
--       con los autores y la URL del whitepaper.
--       Afecta a la subsección que fija el alcance del sistema (Nivel 1 +
--       parcial Nivel 2) y también al Capítulo 7.
--       En cambio "entrenamiento continuo" SÍ está en el whitepaper (17 veces,
--       definido en la pág. de continuous training), así que esa parte está bien.
--
--     [CORREGIR] "la minería de datos educativos viene reportando que sobre
--       datos académicos tabulares los ensambles superan a los modelos lineales"
--       -> romero2020edm no dice eso. CERO apariciones de "ensemble", "forest",
--          "boosting", "bagging", "xgboost", "linear regression" y "logistic
--          regression" en las 30 páginas. Lo único sobre árboles es que los
--          modelos caja blanca "are preferable to black-box models such as
--          neural networks as they are more accurate but less comprehensible",
--          es decir un argumento de INTERPRETABILIDAD que además concede que los
--          de caja negra son más precisos. Es una encuesta metodológica del
--          campo, no un benchmark comparativo.
--
--     [MATIZAR] "El patrón MIND [...] propone además registrar metadatos
--       operativos, como el hash del archivo y la cantidad de filas aceptadas y
--       rechazadas"
--       -> rucco2026mind propone "Ingestion Status and Error Logging: Tracks
--          ingestion progress (e.g., last successful run, records ingested) and
--          logs errors". O sea "records ingested" respalda las filas aceptadas,
--          pero el HASH DEL ARCHIVO no aparece como metadato operativo: en MIND
--          el hash es un método de ingesta incremental (hash-based ingestion).
--          "Filas rechazadas" tampoco figura.
--
--     Verificado correcto: las 27 claves de cita existen en bibliography.bib;
--     el Apéndice ap:formalizacion existe y contiene las tres secciones que el
--     capítulo promete; las fórmulas de riesgo empírico, log-loss, Random
--     Forest, AUC (con el término 1/2 de empates), Brier, BSS y PSI son
--     correctas; la definición de covariate shift vs. concept drift es correcta.
--
-- (B) CONTRA EL CÓDIGO. Hallazgos que no generan SQL:
--
--     [CORREGIR] "el modelo se ajusta minimizando la entropía cruzada"
--       No vale para los tres modelos que el sistema compara. Verificado contra
--       ml_assets.py y los defaults de scikit-learn 1.7.1:
--         LogisticRegression(solver="saga")   -> penalty l2, minimiza log-loss  OK
--         GradientBoostingClassifier(...)     -> loss = log_loss                OK
--         RandomForestClassifier(...)         -> criterion = gini               NO
--       Random Forest no minimiza la entropía cruzada. Elige particiones por
--       impureza de Gini y promedia las proporciones de clase de las hojas.
--
--     [MATIZAR] Ecuación del gradient boosting en el apéndice: le falta el
--       término inicial F_0. La forma de Friedman es F_M(x) = F_0 + suma(...).
--
--     [CONTROL] Umbrales de PSI (0,10 y 0,25): coinciden exactamente con
--       PSI_THRESHOLD_LOW = 0.10 y PSI_THRESHOLD_HIGH = 0.25 de
--       predun_dagster/predun_dagster/drift_utils.py, que además usa
--       N_BINS_DEFAULT = 10 con bins por percentiles de la referencia.
--
--     [CONTROL] Bootstrap agrupado y embargo de maduración: implementados en
--       scripts/backtest_temporal.py (grouped_bootstrap_auc con n=300 y
--       seed=42, y LABEL_HORIZON como embargo). Coinciden con el texto.
--
-- (C) CONTRA LA BASE. Es lo que sigue.


-- -----------------------------------------------------------------------------
-- C3.1  [CONTROL]  El PSI entre entregas vale cero en la ventana cerrada
--
-- TEXTO: "Entre entregas [...] las dos distribuciones describen los mismos
--         hechos, así que el valor esperado del índice es cero exacto.
--         Cualquier valor que se aparte de ahí significa que la fuente corrigió
--         registros viejos o que alguna etapa del pipeline los tocó."
--
-- Es la afirmación teórica más fuerte del capítulo y se sostiene. Sobre la
-- ventana que ambas entregas dan por cerrada el PSI da 0,000001, es decir cero
-- a efectos prácticos. La reconstrucción usa las tablas recon.panel_*.
-- -----------------------------------------------------------------------------
with ref as (
    select materias_en_periodo as v
    from recon.panel_2024_2c
    where at_risk = 1 and academic_period <= '2024_1C'
),
cur as (
    select materias_en_periodo as v
    from recon.panel_2025_1c
    where at_risk = 1 and academic_period <= '2024_1C'
),
b as (select generate_series(0, 9) as i),
edges as (
    select i,
           percentile_cont(i / 10.0)       within group (order by v) as lo,
           percentile_cont((i + 1) / 10.0) within group (order by v) as hi
    from ref, b
    group by i
),
ref_counts as (
    select e.i, count(r.v) as n
    from edges e
    left join ref r on r.v >= e.lo and (r.v < e.hi or (e.i = 9 and r.v <= e.hi))
    group by e.i
),
cur_counts as (
    select e.i, count(c.v) as n
    from edges e
    left join cur c on c.v >= e.lo and (c.v < e.hi or (e.i = 9 and c.v <= e.hi))
    group by e.i
)
select round(sum((cp - rp) * ln(nullif(cp, 0) / nullif(rp, 0)))::numeric, 6)
           as psi_entre_entregas_ventana_cerrada
from (
    select ref_counts.i,
           ref_counts.n::numeric / sum(ref_counts.n) over () as rp,
           cur_counts.n::numeric / sum(cur_counts.n) over () as cp
    from ref_counts join cur_counts using (i)
) x;
-- 0,000001  -> cero a efectos prácticos, tal como predice el capítulo

-- El mismo control detecta que la entrega nueva SÍ tocó el tramo cerrado:
select
    (select count(*) from recon.panel_2024_2c
       where at_risk = 1 and academic_period <= '2024_1C') as filas_entrega_2024_2C,
    (select count(*) from recon.panel_2025_1c
       where at_risk = 1 and academic_period <= '2024_1C') as filas_entrega_2025_1C;
-- 350.061 | 350.107  -> 46 filas de diferencia en un tramo ya cerrado


-- -----------------------------------------------------------------------------
-- C3.2  [CONTROL]  ¿Cuánta señal no lineal hay realmente?
--
-- TEXTO: "Por ese motivo [la regresión logística] funciona como línea base, y la
--         distancia entre ella y los modelos más flexibles mide cuánta señal del
--         problema es realmente no lineal."
--
-- El criterio metodológico es correcto, pero conviene saber qué devuelve. En
-- UNDAV esa distancia es de 0,004 a 0,007 de AUC según el ciclo. Es decir, casi
-- toda la señal del problema es lineal. La ventaja del ensamble es consistente
-- (P(delta>0) = 1) pero de magnitud chica, sobre todo comparada con lo que
-- aporta el modelo completo sobre el baseline de recencia (0,124).
-- -----------------------------------------------------------------------------
select
    cycle_period,
    round(max(roc_auc) filter (where model_name = 'GradientBoosting')::numeric, 4)   as gbm,
    round(max(roc_auc) filter (where model_name = 'RandomForest')::numeric, 4)       as rf,
    round(max(roc_auc) filter (where model_name = 'LogisticRegression')::numeric, 4) as lr,
    round((max(roc_auc) filter (where model_name = 'GradientBoosting')
         - max(roc_auc) filter (where model_name = 'LogisticRegression'))::numeric, 4)
                                                                                     as gap_gbm_lr
from predictions.model_evaluations
group by cycle_period
order by cycle_period;
-- 2024_2C  0,9121  0,9105  0,9054  0,0067
-- 2025_1C  0,9329  0,9296  0,9285  0,0044
-- 2025_2C  0,9304  0,9269  0,9261  0,0043

select comparacion, delta, ci_low, ci_high, p_gt_0
from predictions.delta_auc_paired
order by delta desc;
-- completo − baseline recencia        0,124   [0,1198; 0,1283]  1
-- completo − sin recencia inmediata   0,0173  [0,0160; 0,0187]  1
-- GBM completo − LogisticRegression   0,0044  [0,0036; 0,0052]  1


-- -----------------------------------------------------------------------------
-- C3.3  [CONTROL]  Las métricas que el capítulo describe están implementadas
--
-- TEXTO: el capítulo describe discriminación (AUC, KS), calidad probabilística
--        (Brier, BSS, calibración) y evaluación orientada a la intervención
--        (Precision@K, Recall@K).
--
-- Las tres familias están calculadas y persistidas. No hay métrica declarada en
-- el texto que falte en el sistema.
-- -----------------------------------------------------------------------------
select column_name
from information_schema.columns
where table_schema = 'predictions' and table_name = 'backtest_results'
order by ordinal_position;
-- auc, auc_ci_low, auc_ci_high  -> discriminación con IC por bootstrap agrupado
-- ks                            -> Kolmogórov-Smirnov
-- brier, brier_skill_score      -> calidad probabilística (BSS = 1 - BS/(p(1-p)))
-- calib_slope, calib_intercept, ece -> calibración
-- p_at_5 .. p_at_30             -> Precision@K
-- r_at_5 .. r_at_30             -> Recall@K
-- baseline_recency_auc, prevalence, n_train, n_test


-- -----------------------------------------------------------------------------
-- C3.4  [CORREGIR]  El registro de modelos NO usa etapas
--
-- TEXTO: "Cada candidato pasa por etapas definidas y solo avanza si cumple los
--         umbrales establecidos, lo que deja un historial trazable ante la
--         necesidad de volver a una versión anterior."
--
-- La subsección se presenta como "los [patrones] que usa esta tesis", y este no
-- se usa. Las 62 versiones registradas tienen current_stage = 'None' y no hay
-- ningún alias. En ml_assets.py no se llama nunca a
-- transition_model_version_stage ni a set_registered_model_alias.
--
-- Lo que el sistema SÍ hace: elige el ganador por AUC máximo y registra
-- solamente a ese, con tags (winning_model, registry_version, data_version,
-- thesis_run, staging_periods). Es un gate por selección relativa antes del
-- registro, no una promoción entre etapas contra umbrales absolutos.
--
-- Conexión distinta: la base de MLflow es mlflow_db y requiere el usuario
-- postgres, no siu.
--   docker exec -e PGPASSWORD=postgres predun-postgres psql -U postgres -d mlflow_db
-- -----------------------------------------------------------------------------
-- select current_stage, count(*) from model_versions group by 1;
-- None | 62        -> ninguna versión promovida

-- select count(*) from registered_model_aliases;
-- 0                -> ningún alias

-- select count(*) as experimentos from experiments;   -- 25
-- select count(*) as runs from runs;                  -- 521
-- select name, count(*) as versiones from model_versions group by name;
-- student_dropout_model | 62


-- -----------------------------------------------------------------------------
-- C3.5  [CONTROL]  El stack de la Tabla 3.1 está efectivamente en uso
--
-- Las siete filas de la tabla se corresponden con componentes que operan.
-- PostgreSQL, la ingesta YAML, dbt y Dagster ya quedaron verificados en los
-- bloques C1.12, C1.13 y C2.4. Faltan MLflow y Superset.
-- -----------------------------------------------------------------------------
select
    (select count(*) from predictions.model_evaluations)  as evaluaciones_persistidas,
    (select count(*) from predictions.backtest_results)   as filas_backtest,
    (select count(*) from predictions.drift_metrics)      as mediciones_psi,
    (select count(*) from predictions.latest_predictions) as predicciones_vigentes;

-- MLflow  (mlflow_db, usuario postgres): 25 experimentos, 521 runs,
--         1 modelo registrado con 62 versiones.
-- Superset (base superset, usuario superset): 1 dashboard
--         ("Exploración de Predicciones") con 4 gráficos.
--   docker exec superset_postgres psql -U superset -d superset \
--     -c "select dashboard_title from dashboards;"


-- #############################################################################
-- CAPÍTULO 4 — METODOLOGÍA
-- #############################################################################
--
-- Es el capítulo con más datos verificables. Casi todo reproduce. Lo que sigue
-- son los bloques de verificación y los desvíos encontrados.
--
-- INCONSISTENCIAS INTERNAS (no generan SQL):
--
--   [CORREGIR] "Entre las tres cubren 75.600 legajos únicos" quedó
--     desactualizado respecto del Capítulo 1, que ya dice 75.597. Ver C1.1.
--
--   [CORREGIR] El capítulo dice "catorce años de registros" y más adelante
--     "los quince años de la serie". De 2011_1C a 2025_1C son 14 años.
--
-- CONTRA FUENTE PRIMARIA:
--
--   [OK] losioMacri2015 está citado con precisión. La fuente dice "Esa decisión
--     siempre constituye un corte arbitrario a los fines de operativizar el
--     cálculo de la deserción ya que potencialmente cualquier abandono puede ser
--     transitorio", que es casi literal lo que afirma el texto.
--
--   [OK] mdpi2023 respalda que las variables de trayectoria acumulada son las
--     más predictivas: en su Tabla 5 de permutation importance, "Grade Ranking"
--     y "Completed Credits" salen 1.ª y 2.ª. La fuente aclara igual que
--     "the numbers were not high".
--
--   [MATIZAR] "los estudios sobre el tema coinciden" es un plural sostenido en
--     una sola cita, y además el Capítulo 2 ya matizó esa misma afirmación
--     ("suelen ser las más predictivas, salvo en los trabajos que incorporan
--     datos socioeconómicos detallados"). Conviene alinear las dos.
--
--   [MATIZAR] "no volver a inscribirse de un período al siguiente\cite{coneau}":
--     CONEAU mide reinscripción de un AÑO al siguiente y a la misma carrera, no
--     de un período al siguiente.
--
-- CONTRA EL CÓDIGO:
--
--   [OK] "el sistema no aplica muestreo": el Field train_sample_frac tiene
--     default_value = 1.0. (El docstring de la función dice "default 0.15", que
--     es un comentario desactualizado, pero el default efectivo es 1.0.)
--
--   [OK] Las ventanas: materias_win3 y compañía usan
--     "rows between 3 preceding and current row", o sea el período actual más
--     los tres anteriores, tal como dice el texto. El conjunto de riesgo mira la
--     misma ventana y la etiqueta usa "rows between 1 following and 4 following".
--     Las tres definiciones del texto son exactas.
--
--   [CORREGIR] El linaje del YAML de mapeo NO se registra. El texto afirma que
--     cada entrenamiento queda etiquetado con tres datos: hash de commit,
--     período de entrega y archivo YAML de mapeo. En MLflow existen los tags
--     git_commit y data_version, pero NO hay ningún tag de yaml/mapping/config.
--     Además git_commit está en 4 de 62 versiones y 16 de 521 runs, aunque los
--     tres runs ganadores de los ciclos sí lo llevan. Ver C4.6.
--
--   [CONTROL] Dos de los tres ciclos registran git_dirty = true, es decir que el
--     árbol de trabajo tenía cambios sin commitear al entrenar. Que el sistema
--     lo registre está bien, pero significa que en esos dos ciclos el hash no
--     identifica exactamente el código que corrió.


-- -----------------------------------------------------------------------------
-- C4.1  [CORREGIR]  Legajos con más de una carrera
--
-- TEXTO: "Los 5.355 legajos con inscripción en más de una carrera se
--         representan con una sola fila por período."
--
-- El número 5.355 es correcto, pero corresponde a los legajos que CURSARON en
-- más de una carrera. Los inscriptos en más de una son 7.659.
-- -----------------------------------------------------------------------------
select
    (select count(*) from (
        select legajo from canonical.alumnos
        group by legajo having count(distinct codigo_carrera) > 1) t)  as inscriptos_en_mas_de_una,
    (select count(*) from (
        select legajo from canonical.cursada_historica
        group by legajo having count(distinct cod_carrera) > 1) t)     as cursaron_en_mas_de_una;
-- 7.659 | 5.355


-- -----------------------------------------------------------------------------
-- C4.2  [CORREGIR]  Exceso por doble acta y su tamaño
--
-- TEXTO: "En la entrega 2025_2C eso representa 135.736 filas de exceso, es decir
--         el 14,2\% de la tabla, con una inflación nula hasta 2019 y de entre el
--         34 y el 37\% en cada año desde 2021."
--
-- El exceso es de 135.737 filas, no 135.736 (una de diferencia). El 14,2 % y la
-- inflación nula hasta 2019 se confirman. El rango por año va de 33,9 % a 37,0 %,
-- así que "entre el 34 y el 37" vale solo redondeando a entero.
-- -----------------------------------------------------------------------------
with crudo as (
    select * from staging.cursada_historica_flat
    where academic_period = '2025_2C'
      and cod_carrera in (
          'SA5','AB5','AF3','PT2','PT7','AF1','CA2','AB1','TA06','PT6',
          'TA1','SA2','PT8','CA1','CS2','AF0','CA4','PT1','SA1','CS0',
          'CS3','PT3','CS1','CS9','CS4','AB4','PT9','SA4','PT4')
)
select
    count(*)                                                   as filas_crudas,
    (select count(*) from canonical.cursada_historica)         as eventos_dedup,
    count(*) - (select count(*) from canonical.cursada_historica) as exceso,
    round(100.0 * (count(*) - (select count(*) from canonical.cursada_historica))
          / count(*), 2)                                       as pct_exceso
from crudo;
-- 954.774 | 819.037 | 135.737 | 14,22 %

-- Inflación por año:
with crudo as (
    select left(trim(fecha), 4) as anio, count(*) as n
    from staging.cursada_historica_flat
    where academic_period = '2025_2C'
      and cod_carrera in (
          'SA5','AB5','AF3','PT2','PT7','AF1','CA2','AB1','TA06','PT6',
          'TA1','SA2','PT8','CA1','CS2','AF0','CA4','PT1','SA1','CS0',
          'CS3','PT3','CS1','CS9','CS4','AB4','PT9','SA4','PT4')
      and trim(fecha) ~ '^[0-9]{4}-'
    group by 1
),
dedup as (
    select left(trim(fecha), 4) as anio, count(*) as n
    from canonical.cursada_historica
    where trim(fecha) ~ '^[0-9]{4}-'
    group by 1
)
select c.anio, c.n as crudo, d.n as dedup,
       round(100.0 * (c.n - d.n) / d.n, 1) as inflacion_pct
from crudo c join dedup d using (anio)
where c.anio >= '2018'
order by c.anio;
-- 2018 0,0 | 2019 0,0 | 2020 0,0 | 2021 36,6 | 2022 33,9
-- 2023 33,9 | 2024 37,0 | 2025 36,9


-- -----------------------------------------------------------------------------
-- C4.3  [CORREGIR]  Tabla del cambio de vocabulario (tab:vocabulario)
--
-- El argumento del capítulo se sostiene por completo: las categorías se
-- intercambian por pares en 2021_1C y los agregados quedan estables. Pero las
-- celdas no reproducen exacto contra la base actual, con desvíos de hasta
-- 0,9 puntos. Los valores de abajo son los que devuelve el snapshot vigente,
-- usando la MISMA regla de período que student_panel.sql (enero y febrero caen
-- en el 2C del año anterior).
-- -----------------------------------------------------------------------------
with p as (
    select case
             when extract(month from trim(fecha)::date) between 3 and 8
               then extract(year from trim(fecha)::date)::int || '_1C'
             when extract(month from trim(fecha)::date) >= 9
               then extract(year from trim(fecha)::date)::int || '_2C'
             else (extract(year from trim(fecha)::date)::int - 1) || '_2C'
           end as periodo,
           resultado
    from canonical.cursada_historica
    where trim(coalesce(fecha, '')) ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
)
select
    periodo,
    round(100.0 * count(*) filter (where resultado ilike 'Promoc%') / count(*), 1)  as promociono,
    round(100.0 * count(*) filter (where resultado = 'Regular') / count(*), 1)      as regular,
    round(100.0 * count(*) filter (where resultado ilike 'Promoc%'
                                      or resultado = 'Regular') / count(*), 1)      as p_mas_r,
    round(100.0 * count(*) filter (where resultado ilike 'Abandon%') / count(*), 1) as abandono,
    round(100.0 * count(*) filter (where resultado = 'Libre') / count(*), 1)        as libre,
    round(100.0 * count(*) filter (where resultado ilike 'Abandon%'
                                      or resultado = 'Libre') / count(*), 1)        as a_mas_l
from p
where periodo in ('2019_1C', '2020_2C', '2021_1C', '2022_2C', '2025_1C')
group by periodo
order by periodo;
--          tesis                        base
-- 2019_1C  37,8 16,9 54,7 29,0 12,1 41,1 -> 38,0 16,7 54,8 28,8 12,2 41,0
-- 2020_2C  44,6 15,1 59,7 31,2  7,8 38,9 -> 44,6 15,1 59,6 32,0  6,9 38,9
-- 2021_1C  10,8 46,1 56,9  7,8 29,4 37,2 -> 11,2 45,5 56,7  8,0 29,4 37,5
-- 2022_2C   8,0 45,3 53,3  5,3 35,7 41,0 ->  7,9 45,3 53,3  5,3 35,7 41,0
-- 2025_1C   7,3 47,0 54,3  8,4 32,2 40,7 ->  7,3 47,1 54,3  8,4 32,3 40,7
--
-- El denominador incluye los seis valores de resultado (se suman Insuficiente y
-- No Promocionó), por eso P+R y A+L no llegan a 100.


-- -----------------------------------------------------------------------------
-- C4.4  [CORREGIR]  Los dos indicadores independientes del campo resultado
--
-- TEXTO: "43,0\% en 2019_2C, 49,0\% en 2020_2C, 48,6\% en 2021_1C y 44,6\% en
--         2025_1C" para la proporción de cursadas con nota >= 7; y "Regular
--         tenía una nota media de 5,71, y después de 2021 sube a 7,46 [...]
--         Promocionó, que va de 8,08 a 8,19".
--
-- El argumento se sostiene (no hay acantilado en la nota, y el Regular posterior
-- absorbe a la población de Promocionó). Tres de los cuatro valores de nota
-- media son exactos. Los de nota >= 7 desvían hasta 0,7 puntos y el Regular
-- previo a 2021 da 5,68, no 5,71.
-- -----------------------------------------------------------------------------
with p as (
    select case
             when extract(month from trim(fecha)::date) between 3 and 8
               then extract(year from trim(fecha)::date)::int || '_1C'
             when extract(month from trim(fecha)::date) >= 9
               then extract(year from trim(fecha)::date)::int || '_2C'
             else (extract(year from trim(fecha)::date)::int - 1) || '_2C'
           end as periodo,
           case when trim(nota) ~ '^[0-9]+([.,][0-9]+)?$'
                then replace(trim(nota), ',', '.')::numeric end as n
    from canonical.cursada_historica
    where trim(coalesce(fecha, '')) ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
)
select periodo, count(*) as total_cursadas,
       round(100.0 * count(*) filter (where n >= 7) / count(*), 1) as pct_nota_ge7
from p
where periodo in ('2019_2C', '2020_2C', '2021_1C', '2025_1C')
group by periodo
order by periodo;
-- 2019_2C 42,8 (tesis 43,0) | 2020_2C 48,3 (49,0)
-- 2021_1C 48,3 (48,6)       | 2025_1C 44,7 (44,6)
-- El denominador son TODAS las cursadas del período, no solo las que traen nota.

with p as (
    select case when trim(fecha)::date < date '2021-03-01'
                then 'antes de 2021_1C' else 'desde 2021_1C' end as tramo,
           resultado,
           case when trim(nota) ~ '^[0-9]+([.,][0-9]+)?$'
                then replace(trim(nota), ',', '.')::numeric end as n
    from canonical.cursada_historica
    where trim(coalesce(fecha, '')) ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
)
select resultado, tramo,
       count(*) filter (where n is not null) as con_nota,
       round(avg(n), 2) as nota_media
from p
where resultado in ('Regular', 'Promocionó')
group by resultado, tramo
order by resultado, tramo;
-- Promocionó antes 8,08 (tesis 8,08)  OK
-- Promocionó desde 8,19 (tesis 8,19)  OK
-- Regular    antes 5,68 (tesis 5,71)  <-- desvío
-- Regular    desde 7,46 (tesis 7,46)  OK


-- -----------------------------------------------------------------------------
-- C4.5  [CORREGIR]  El módulo INDEC tiene dos de sus tres tablas vacías
--
-- TEXTO: "El módulo predun_indec ya descarga las series de inflación, desempleo
--         y actividad económica desde las APIs del organismo y las carga en la
--         base de datos" y "en esta versión los datos del INDEC ya están
--         cargados en la base pero no alimentan al modelo".
--
-- El CÓDIGO existe para las tres series (predun_indec/indec_data_pipeline.py
-- tiene las funciones download_and_load_ipc_categories,
-- download_and_load_unemployment_rates y la de EMAE, con las URLs de
-- apis.datos.gob.ar). Pero en la base solo hay datos de actividad económica.
-- Inflación y desempleo están vacías, así que la afirmación de estado es falsa
-- para dos de las tres series.
-- -----------------------------------------------------------------------------
select 'ipc_categories (inflación)' as tabla, count(*) as filas from canonical.ipc_categories
union all
select 'unemployment_rates (desempleo)', count(*) from canonical.unemployment_rates
union all
select 'emae_indicators (actividad)',   count(*) from canonical.emae_indicators
order by 1;
-- emae_indicators (actividad)      100
-- ipc_categories (inflación)         0   <-- vacía
-- unemployment_rates (desempleo)     0   <-- vacía


-- -----------------------------------------------------------------------------
-- C4.6  [CORREGIR]  Linaje del entrenamiento en MLflow
--
-- TEXTO: "Cada entrenamiento queda etiquetado con tres datos que juntos permiten
--         repetirlo, que son el hash del commit de Git, el período de entrega de
--         los datos y el archivo YAML de mapeo que le corresponde a ese período."
--
-- Los dos primeros existen. El tercero no se registra en ningún lado.
-- Conexión: mlflow_db con el usuario postgres.
--   docker exec -e PGPASSWORD=postgres predun-postgres psql -U postgres -d mlflow_db
-- -----------------------------------------------------------------------------
-- select distinct key from model_version_tags order by 1;
--   data_version | git_commit | git_dirty | thesis_run     -> no hay yaml/mapping

-- select distinct key from tags where key ~* 'yaml|mapping|config|map';
--   0 filas

-- select key, count(*) from model_version_tags group by 1;
--   data_version 32 | git_commit 4 | git_dirty 4 | thesis_run 32   (sobre 62 versiones)

-- Los tres runs ganadores de los ciclos sí llevan el linaje disponible:
--   2024_2C  git_commit 110b1007...  git_dirty true   staging_periods 2024_2C
--   2025_1C  git_commit 110b1007...  git_dirty true   staging_periods 2024_2C,2025_1C
--   2025_2C  git_commit 6631e24b...  git_dirty false  staging_periods 2024_2C,2025_1C,2025_2C
--
-- Los run_id salen de:
select cycle_period, model_name, mlflow_run_id
from predictions.model_evaluations
where is_selected_model
order by cycle_period;


-- -----------------------------------------------------------------------------
-- C4.7  [CONTROL]  Estructura del panel y períodos
--
-- Confirma las cifras del capítulo: 687.186 filas, 394.362 en riesgo, 304.175
-- con etiqueta observable dentro del conjunto de riesgo, y 29 períodos entre
-- 2011_1C y 2025_1C (catorce años).
-- -----------------------------------------------------------------------------
select
    count(*)                                                        as filas_panel,
    count(*) filter (where at_risk = 1)                             as conjunto_de_riesgo,
    count(*) filter (where at_risk = 1 and dropout_next is not null) as etiquetadas_en_riesgo,
    count(distinct academic_period)                                 as periodos,
    min(academic_period)                                            as primer_periodo,
    max(academic_period)                                            as ultimo_periodo
from marts.student_panel;
-- 687.186 | 394.362 | 304.175 | 29 | 2011_1C | 2025_1C

-- La tabla de sensibilidad del horizonte (tab:horizon_sensitivity) reproduce
-- exacto contra predictions.sensitivity_horizon. Ver también C1.11.
select horizonte_periodos, anios, abandono_legajos, tasa_reactivacion
from predictions.sensitivity_horizon
order by horizonte_periodos;
-- 2  1,0  35.979  0,1547     3  1,5  33.648  0,1101
-- 4  2,0  31.047  0,0843     6  3,0  26.209  0,0555
-- La caída de 4 a 6 períodos deja afuera 4.838 legajos ("casi 5.000").


-- #############################################################################
-- CAPÍTULO 5 — SOLUCIÓN Y RESULTADOS
-- #############################################################################
--
-- Es el capítulo con más datos y el que mejor reproduce. Cuatro tablas salen
-- exactas celda por celda. Los desvíos están abajo y son todos de milésimas,
-- salvo dos.
--
-- REPRODUCEN EXACTO (no necesitan corrección):
--   tab:model_metrics      32/32 celdas contra predictions.backtest_results
--   tab:drift_metrics      11 filas x 6 columnas contra predictions.drift_metrics
--   tab:fairness           10 filas x 6 columnas contra predictions.fairness_subgroups
--   tab:graduation_sensitivity  8/8 celdas contra predictions.sensitivity_graduation
--   Calibración (slope 0,92-1,11, intercepto -0,02 a 0,35, ECE 0,018-0,040)
--   Actas por mes (julio 44,2 %, diciembre 36,4 %)
--   Deriva por ciclo (cod_carrera 0,283/0,261/0,262; materias_en_periodo
--     0,101/0,104/0,046), incluida la referencia móvil del primer ciclo
--   Scoring (19.927 legajos, prob. media 0,382, 7.997 sobre 0,5, 40,1 %)
--   Monotonía por recencia (0,15 / 0,67 / 0,81 / 0,84)
--   Sexo (39.787 F, 29.945 M, 63,7 %, 67,2 %, 46 otras identidades, 1 contradictorio)
--   Permutation importance (0,071 / 0,046 / 0,023 / 0,012 / 0,005)
--   Dispersión por carrera (21 carreras, AUC 0,869 a 0,960, rango 0,091)
--   Toda la sección de comparación entre entregas (ver C5.4)
--   Precisión del primer origen (0,896 en top-5 % a 0,783 en top-30 %)
--   Bootstrap pareado con 500 réplicas (ablation_and_paired.py, n=500, seed=42)


-- -----------------------------------------------------------------------------
-- C5.1  [CORREGIR]  tab:ablation
--
-- Tres de las ocho celdas desvían. La del baseline es la más grande.
-- El texto que acompaña también dice "llega a un AUC de 0,914", que es 0,913.
-- -----------------------------------------------------------------------------
select variante, n_features, round(auc::numeric, 4) as auc, round(p_at_10::numeric, 4) as p_at_10
from predictions.ablation_recency
order by auc desc;
-- variante                        tesis          base
-- completo                        0,930 / 0,953  0,9305 / 0,9548   OK
-- sin dias_desde_ult_actividad    0,927 / 0,938  0,9273 / 0,9362   OK
-- sin recencia inmediata          0,914 / 0,937  0,9132 / 0,9341   -> 0,913 / 0,934
-- baseline recencia               0,806 / 0,864  0,8064 / 0,8771   -> 0,806 / 0,877


-- -----------------------------------------------------------------------------
-- C5.2  [CORREGIR]  tab:auc_por_recencia
--
-- Siete de las veinte celdas desvían entre 0,001 y 0,006. El texto también dice
-- "Precision@10\% de 0,731" (es 0,734) y "el AUC baja a entre 0,761 y 0,837"
-- (es entre 0,760 y 0,834).
-- -----------------------------------------------------------------------------
select situacion, dias, n, round(prevalencia::numeric, 3) as prev,
       round(auc::numeric, 4) as auc, round(p_at_10::numeric, 4) as p_at_10,
       round(lift::numeric, 2) as lift
from predictions.auc_por_recencia
order by dias;
-- situacion                  tesis AUC/P@10   base AUC/P@10
-- Cursó en el período        0,904 / 0,731    0,9045 / 0,7344  -> P@10 0,734
-- 1 cuatrimestre inactivo    0,761 / 0,874    0,7600 / 0,8679  -> 0,760 / 0,868
-- 2 cuatrimestres inactivos  0,810 / 0,972    0,8093 / 0,9752  -> 0,809 / 0,975
-- 3 cuatrimestres inactivos  0,837 / 0,952    0,8335 / 0,9490  -> 0,834 / 0,949
-- N y prevalencia reproducen exacto en los cuatro grupos.


-- -----------------------------------------------------------------------------
-- C5.3  [CORREGIR]  tab:paired_auc
--
-- Los tres deltas y sus seis extremos de intervalo desvían en la cuarta cifra.
-- Ojo con una inconsistencia interna: el texto dice "de 0,124 puntos", que sí
-- coincide con la base, mientras la tabla dice 0,1239.
-- -----------------------------------------------------------------------------
select comparacion, round(delta::numeric, 4) as delta,
       round(ci_low::numeric, 4) as ci_low, round(ci_high::numeric, 4) as ci_high, p_gt_0
from predictions.delta_auc_paired
order by delta desc;
-- comparación                       tesis                      base
-- completo − baseline recencia      0,1239 [0,1197; 0,1282]    0,1240 [0,1198; 0,1283]
-- completo − sin recencia inmediata 0,0168 [0,0155; 0,0181]    0,0173 [0,0160; 0,0187]
-- GBM − LogisticRegression          0,0043 [0,0035; 0,0050]    0,0044 [0,0036; 0,0052]


-- -----------------------------------------------------------------------------
-- C5.4  [CORREGIR]  tab:model_comparison y tab:precision_at_k
--
-- Dos celdas en la primera y una en la segunda.
-- El texto dice además que el RandomForest "entrena en la quinta parte del
-- tiempo": con 12 s contra 2 s es la sexta parte.
-- -----------------------------------------------------------------------------
select model_name,
       round(roc_auc::numeric, 3)               as auc,
       round(brier_score::numeric, 4)           as brier,
       round(ks_statistic::numeric, 3)          as ks,
       round(f1_dropout::numeric, 3)            as f1,
       round(precision_dropout::numeric, 4)     as prec,
       round(recall_dropout::numeric, 4)        as recall,
       round(training_time_seconds::numeric, 1) as t_seg,
       round(average_precision::numeric, 3)     as ap
from predictions.model_evaluations
where cycle_period = '2025_2C'
order by roc_auc desc;
-- GBM  t = 11,8 s -> 12 s (la tesis dice 11)
-- LR   recall = 0,7965 -> 0,796 (la tesis dice 0,797)
-- El resto de la tabla y el Average Precision 0,894 reproducen exacto.

select round(p_at_5::numeric,3)  as p5,  round(r_at_5::numeric,3)  as r5,
       round(p_at_10::numeric,3) as p10, round(r_at_10::numeric,3) as r10,
       round(p_at_20::numeric,3) as p20, round(r_at_20::numeric,3) as r20,
       round(p_at_30::numeric,3) as p30, round(r_at_30::numeric,3) as r30
from predictions.backtest_results
where test_period = '2023_1C';
-- P@5 = 0,960 (la tesis dice 0,959). Las otras siete celdas son exactas.


-- -----------------------------------------------------------------------------
-- C5.5  [CONTROL]  La comparación entre entregas reproduce entera
--
-- Todos los números de sec:comparacion_results salen exactos. Se dejan las
-- consultas porque son el control más difícil de reconstruir del capítulo.
-- -----------------------------------------------------------------------------
-- Coincidencia de la ventana cerrada entre 2025_1C y 2025_2C:
select (select count(*) from recon.panel_2025_1c
          where at_risk = 1 and academic_period <= '2024_2C') as filas_entrega_2025_1C,
       (select count(*) from recon.panel_2025_2c
          where at_risk = 1 and academic_period <= '2024_2C') as filas_entrega_2025_2C;
-- 372.223 | 372.223

-- Conciliación por evento con clave estable (bajas 100 y 37):
create temp table ev as
    select academic_period as entrega, evento_hash,
           md5(concat_ws('|', legajo, cod_carrera, cod_materia, anio,
                         tipo_cursada, nota, fecha, resultado)) as hecho_hash
    from canonical.cursada_historica_history
    union all
    select academic_period, evento_hash,
           md5(concat_ws('|', legajo, cod_carrera, cod_materia, anio,
                         tipo_cursada, nota, fecha, resultado))
    from canonical.cursada_historica;

select entrega, count(*) as eventos from ev group by entrega order by entrega;
-- 2024_2C 732.276 | 2025_1C 774.231 | 2025_2C 819.037  ("más de 700.000")

with p as (
    select hecho_hash as k,
           bool_or(entrega = '2024_2C') as d1,
           bool_or(entrega = '2025_1C') as d2,
           bool_or(entrega = '2025_2C') as d3
    from ev group by 1)
select count(*) filter (where d1 and not d2) as baja_1a2,
       count(*) filter (where d2 and not d3) as baja_2a3
from p;
-- 100 | 37   -> 0,014 % y 0,005 %

-- [CORREGIR] El defecto de la clave: la tesis dice 41.746 eventos.
with p as (
    select evento_hash as k,
           bool_or(entrega = '2024_2C') as d1,
           bool_or(entrega = '2025_1C') as d2
    from ev group by 1)
select count(*) filter (where d1 and not d2) as bajas_por_evento_hash
from p;
-- 41.867 brutas (41.767 netas de las 100 bajas reales). La tesis dice 41.746.
-- El 5,7 % de la tabla se sostiene con cualquiera de los tres valores.

-- El archivo de avance defectuoso de 2024_2C:
select academic_period, count(*) as filas, count(distinct legajo) as legajos
from staging.porcentaje_avance_flat group by 1 order by 1;
-- 2024_2C 2.961 / 2.800 | 2025_1C 130.733 | 2025_2C 134.437   ("más de 130.000")

select academic_period, count(*) as filas_canonical, count(distinct legajo) as legajos,
       count(*) filter (where replace(trim(porcentaje_avance), ',', '.')::numeric = 100) as en_100
from canonical.porcentaje_avance_history
where academic_period = '2024_2C'
  and trim(porcentaje_avance) ~ '^[0-9]+([.,][0-9]+)?$'
group by 1;
-- 2.593 filas | 2.457 legajos | 2.593 en 100,0 %  -> todos exactamente 100 %

-- Legajos en finalización estimada por entrega:
select academic_period, count(distinct legajo) as legajos_ge90
from canonical.porcentaje_avance_history
where trim(porcentaje_avance) ~ '^[0-9]+([.,][0-9]+)?$'
  and replace(trim(porcentaje_avance), ',', '.')::numeric >= 90
group by 1
union all
select academic_period, count(distinct legajo)
from canonical.porcentaje_avance
where trim(porcentaje_avance) ~ '^[0-9]+([.,][0-9]+)?$'
  and replace(trim(porcentaje_avance), ',', '.')::numeric >= 90
group by 1
order by 1;
-- 2024_2C 2.457 | 2025_1C 4.072 | 2025_2C 4.364

-- [MATIZAR] "un archivo que cubría el 4,8 % de los legajos":
select (select count(distinct legajo) from canonical.alumnos_history
          where academic_period = '2024_2C')                              as padron_2024_2C,
       round(100.0 * 2961 / (select count(distinct legajo) from canonical.alumnos_history
          where academic_period = '2024_2C'), 1)                          as pct_por_filas_crudas,
       round(100.0 * 2800 / (select count(distinct legajo) from canonical.alumnos_history
          where academic_period = '2024_2C'), 1)                          as pct_por_legajos_staging,
       round(100.0 * 2457 / (select count(distinct legajo) from canonical.alumnos_history
          where academic_period = '2024_2C'), 1)                          as pct_por_legajos_canonical;
-- 61.989 | 4,8 % | 4,5 % | 4,0 %
-- El 4,8 % sale de comparar FILAS crudas contra LEGAJOS del padrón. Por legajos
-- distintos da 4,5 %, y por los que pasan el filtro de carreras da 4,0 %.

-- El período frontera vacío:
with e as (
    select academic_period as entrega,
           case
             when extract(month from trim(fecha)::date) between 3 and 8
               then extract(year from trim(fecha)::date)::int || '_1C'
             when extract(month from trim(fecha)::date) >= 9
               then extract(year from trim(fecha)::date)::int || '_2C'
             else (extract(year from trim(fecha)::date)::int - 1) || '_2C'
           end as periodo
    from canonical.cursada_historica_history
    where trim(coalesce(fecha, '')) ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$')
select entrega, periodo, count(*) as eventos
from e
where (entrega = '2024_2C' and periodo = '2024_2C')
   or (entrega = '2025_1C' and periodo = '2025_1C')
group by 1, 2 order by 1;
-- 2024_2C abre su período con 22 eventos | 2025_1C con 248

select (select count(*) from recon.panel_2024_2c
          where at_risk = 1 and academic_period = '2024_2C') as riesgo_2024_2C,
       (select count(*) from recon.panel_2025_1c
          where at_risk = 1 and academic_period = '2025_1C') as riesgo_2025_1C;
-- 20.127 | 19.202


-- -----------------------------------------------------------------------------
-- C5.6  [RESUELTO]  Importancia por impureza, ahora persistida
--
-- Era el único dato del capítulo sin respaldo en la base. Los valores 0,62,
-- 0,11 y "cerca del 73 %" existían solo dentro de la figura.
--
-- Se recalcularon desde el modelo registrado (versión 62) y se cargaron en la
-- tabla nueva predictions.feature_importance, con el mismo criterio que
-- predictions.permutation_importance. Son 38 filas (las 11 variables, con
-- cod_carrera expandida en one-hot) y suman 1,0 exacto.
-- -----------------------------------------------------------------------------
select feature_name, round(importance::numeric, 4) as importancia
from predictions.feature_importance
where cycle_period = '2025_2C'
order by importance desc
limit 6;
-- num__aprob_en_periodo          0,6177   -> la tesis dice 0,62
-- num__dias_desde_ult_actividad  0,1107   -> la tesis dice 0,11
-- num__materias_cum              0,0743   -> tercero por impureza, como dice el texto
-- num__materias_en_periodo       0,0556   -> la tesis dice "pesa 0,06 por impureza"

select round(sum(importance)::numeric, 4) as suma_top2
from (select importance from predictions.feature_importance
      where cycle_period = '2025_2C' order by importance desc limit 2) t;
-- 0,7284  -> la tesis dice "cerca del 73 %"

-- La tabla se reconstruye con (entorno eda-predun, desde la raíz del repo):
--
--   import pickle, numpy as np
--   p = "mlflow/mlflow_data/2/models/m-579092a537b944ed9bcbf5cdb0b6ae08/artifacts/model.pkl"
--   m = pickle.load(open(p, "rb"))
--   imp = m.steps[-1][1].feature_importances_
--   names = m[:-1].get_feature_names_out()
--
-- El id del artefacto sale de:
--   docker exec -e PGPASSWORD=postgres predun-postgres psql -U postgres -d mlflow_db \
--     -c "select source from model_versions where version='62';"


-- #############################################################################
-- CAPÍTULO 6 — DISCUSIÓN
-- #############################################################################
--
-- Capítulo argumentativo. Re-cita datos de los capítulos anteriores y suma dos
-- afirmaciones empíricas propias que hasta ahora no tenían consulta. Las dos
-- se sostienen y quedan abajo.
--
-- INCONSISTENCIA INTERNA:
--
--   [CORREGIR] "el hash de 41.746 eventos cambió" quedó desactualizado respecto
--     del Capítulo 5, que ya dice 41.767. Ver C5.5.
--
-- CONTRA EL CÓDIGO:
--
--   [CORREGIR] "pruebas de dbt que corren en cada materialización sobre
--     unicidad, integridad referencial y magnitud del cambio"
--     NO hay tests de integridad referencial. Un grep de `relationships` sobre
--     predun_dbt/models/*/schema.yml no devuelve nada. Los 22 tests declarados
--     son 14 not_null, 4 accepted_values, 2 unique y 2
--     unique_combination_of_columns, más el test singular
--     tests/assert_no_count_inflation.sql, que es el de magnitud del cambio.
--     La enumeración correcta sería unicidad, no nulidad, valores aceptados y
--     magnitud del cambio.
--
--   [OK] "un control explícito de vocabulario en la capa canonical, análogo al
--     que ya existe para los códigos de carrera": el test accepted_values está
--     efectivamente declarado sobre cod_carrera y sobre origen.
--
-- REPRODUCEN EXACTO (verificados en capítulos anteriores, no se repiten acá):
--   AUC 0,926 contra 0,930 y la franja de cuatro milésimas; los tres milésimos
--   entre GradientBoosting y RandomForest; las 23.685 observaciones de prueba;
--   el reparto de métricas entre GBM (AUC, KS, Brier, precisión) y RF (F1,
--   recall); la brecha por cohorte (0,937 a 0,887, TPR 0,904 a 0,455,
--   prevalencia 0,278); las 2.961 filas del archivo de avance con todos sus
--   valores en 100 %; los 0,18 puntos de inflación del scoring; y que las
--   variables aprob_* quedan muy por debajo del umbral moderado de deriva.


-- -----------------------------------------------------------------------------
-- C6.1  [CONTROL]  El quiebre es de tiempo calendario, no de trayectoria
--
-- TEXTO: "Las cohortes viejas caen igual que las nuevas, y los estudiantes con
--         más de cinco años de trayectoria, que por definición ya acumularon
--         promociones, no pueden caer por un efecto de maduración. El quiebre es
--         de tiempo calendario y no de tiempo de trayectoria."
--
-- Es el argumento central del capítulo y no tenía consulta que lo respaldara.
-- Se sostiene con holgura. La caída de la promoción entre 2020 y 2021 ocurre en
-- los dos estratos de antigüedad, y de hecho es MAYOR en el de trayectoria
-- larga, que es justo donde la explicación por maduración no puede operar.
-- El agregado de cursadas aprobadas, en cambio, se mantiene estable en ambos.
-- -----------------------------------------------------------------------------
with ini as (
    select legajo, min(trim(fecha)::date) as f0
    from canonical.cursada_historica
    where trim(coalesce(fecha, '')) ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
    group by legajo
),
e as (
    select left(trim(c.fecha), 4) as anio,
           case when (trim(c.fecha)::date - i.f0) / 365.25 >= 5
                then '5+ anios trayectoria' else '<5 anios trayectoria' end as estrato,
           c.resultado
    from canonical.cursada_historica c
    join ini i using (legajo)
    where trim(coalesce(c.fecha, '')) ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
      and left(trim(c.fecha), 4) between '2019' and '2022'
)
select estrato, anio, count(*) as n,
       round(100.0 * count(*) filter (where resultado ilike 'Promoc%') / count(*), 1)
           as pct_promociono,
       round(100.0 * count(*) filter (where resultado ilike 'Promoc%'
                                         or resultado = 'Regular') / count(*), 1)
           as pct_aprobado
from e
group by estrato, anio
order by estrato, anio;
-- <5 anios  2019 37,5 / 54,4   2020 43,8 / 58,2   2021 10,4 / 54,4   2022  7,9 / 51,8
-- 5+ anios  2019 41,4 / 56,3   2020 51,1 / 61,9   2021 11,7 / 58,1   2022  8,8 / 54,3
--
-- Caída de la promoción entre 2020 y 2021:
--   menos de 5 años  -33,4 puntos
--   5 años o más     -39,4 puntos   <-- cae MÁS donde no puede haber maduración
-- El predicado agregado se mantiene estable en los dos estratos.


-- -----------------------------------------------------------------------------
-- C6.2  [CONTROL]  La comparación entre entregas no habría visto la duplicación
--
-- TEXTO: "La comparación no habría detectado la duplicación de actas de
--         2021_1C, y se verificó con datos que no lo habría hecho, porque las
--         tres entregas la traen en la misma proporción."
--
-- Correcto. La inflación por doble acta desde 2021 es prácticamente idéntica en
-- las tres entregas, con 0,5 puntos de rango, así que al comparar una entrega
-- contra la anterior el defecto se cancela. Es el mismo modo de falla que el
-- capítulo describe para el monitoreo dentro de una entrega.
-- -----------------------------------------------------------------------------
with crudo as (
    select academic_period as entrega, count(*) as filas
    from staging.cursada_historica_flat
    where trim(fecha) ~ '^[0-9]{4}-' and left(trim(fecha), 4) >= '2021'
      and cod_carrera in (
          'SA5','AB5','AF3','PT2','PT7','AF1','CA2','AB1','TA06','PT6',
          'TA1','SA2','PT8','CA1','CS2','AF0','CA4','PT1','SA1','CS0',
          'CS3','PT3','CS1','CS9','CS4','AB4','PT9','SA4','PT4')
    group by 1
),
dedup as (
    select academic_period as entrega,
           count(distinct md5(concat_ws('|', legajo, cod_carrera, cod_materia, anio,
                                        tipo_cursada, nota, fecha, resultado))) as eventos
    from staging.cursada_historica_flat
    where trim(fecha) ~ '^[0-9]{4}-' and left(trim(fecha), 4) >= '2021'
      and cod_carrera in (
          'SA5','AB5','AF3','PT2','PT7','AF1','CA2','AB1','TA06','PT6',
          'TA1','SA2','PT8','CA1','CS2','AF0','CA4','PT1','SA1','CS0',
          'CS3','PT3','CS1','CS9','CS4','AB4','PT9','SA4','PT4')
    group by 1
)
select c.entrega, c.filas, d.eventos,
       round(100.0 * (c.filas - d.eventos) / d.eventos, 1) as inflacion_pct
from crudo c join dedup d using (entrega)
order by c.entrega;
-- 2024_2C  398.446 / 294.966  35,1 %
-- 2025_1C  456.121 / 336.839  35,4 %
-- 2025_2C  517.369 / 381.645  35,6 %


-- -----------------------------------------------------------------------------
-- C6.3  [CONTROL]  Deriva por segmento
--
-- TEXTO: "Una verificación preliminar por sexo registrado y por cohorte de
--         ingreso arroja valores uniformemente bajos, sin evidencia de deriva
--         localizada."
--
-- Correcto. Los doce valores quedan por debajo del umbral moderado de 0,10.
-- Conviene tener presente que psi_dias en la cohorte 2018-2020 da 0,094, que
-- está al borde, mientras el resto va de 0,004 a 0,033.
-- -----------------------------------------------------------------------------
select segmento, n_ref, n_act, psi_aprob_rate_win3, psi_dias, psi_dropout_next
from predictions.drift_by_segment
order by segmento;
-- cohorte=<=2017     0,007 | 0,013 | 0,033
-- cohorte=2018-2020  0,021 | 0,094 | 0,004
-- sexo=F             0,007 | 0,021 | 0,010
-- sexo=M             0,007 | 0,019 | 0,006


-- #############################################################################
-- CAPÍTULO 7 — CONCLUSIONES
-- #############################################################################
--
-- Capítulo de cierre. Casi todo re-cita datos ya verificados. Aporta dos
-- experimentos que no aparecían en los capítulos anteriores (C7.1 y C7.2) y
-- los dos reproducen exacto.
--
-- HALLAZGOS:
--
--   [CORREGIR] "cada versión del modelo en el registro queda vinculada
--     automáticamente al hash del commit del código que la entrenó, al período
--     de datos y al run de entrenamiento". Es una afirmación absoluta que no se
--     sostiene: sobre 62 versiones registradas, solo 4 tienen el tag git_commit
--     y 32 tienen data_version. Los tres runs ganadores de los ciclos sí llevan
--     el linaje completo (ver C4.6), pero "cada versión" no. El Capítulo 4 ya
--     se corrigió y este quedó con la formulación vieja.
--
--   [MATIZAR] "el desempeño sobre estudiantes no observados no se degrada, con
--     un AUC de 0,928". El 0,9277 es el AUC del backtest con partición agrupada
--     evaluado sobre TODO el conjunto de prueba, no el AUC sobre los estudiantes
--     no observados. Ese es auc_new = 0,9351, que además es MEJOR que el global
--     de 0,9304. El argumento del texto es más fuerte de lo que el texto dice.
--
--   [MATIZAR] "la mayoría del sistema universitario argentino comparte el mismo
--     sistema de gestión académica" va sin cita, igual que la afirmación
--     equivalente del Capítulo 2. El sitio oficial del SIU informa que "más de
--     150 instituciones universitarias y organismos" usan SIU-Guaraní.
--
-- REPRODUCEN EXACTO: los 2,9 millones de staging; el panel de 687.186 y los
--   304.175 de modelado; "más de la mitad" en estado de abandono (56,1 % sobre
--   los legajos con cursada, 65,2 % sobre el padrón); el AUC de 0,930; el 73 %
--   de importancia concentrada (0,7284); la franja de cuatro milésimas y el
--   0,926 de la logística; la Precision@10 % de 95,3 %; los 3,5 puntos de
--   diferencial por sexo; los 0,18 puntos de inflación del scoring; la
--   correlación de rangos de 0,83 y el solapamiento del decil entre 92 y 95 %.


-- -----------------------------------------------------------------------------
-- C7.1  [CONTROL]  El experimento de test fijo
--
-- TEXTO: "dicho control mostró que sumar tres cuatrimestres de datos mueve el
--         AUC 0,0006 con intervalos solapados, de modo que el valor del
--         reentrenamiento periódico está en incorporar los períodos cuya
--         etiqueta acaba de madurar y no en acumular volumen."
--
-- Exacto. Con el período de prueba fijo en 2023_1C, mover el corte de
-- entrenamiento de 2019_2C a 2021_1C (tres cuatrimestres más, de 155.141 a
-- 213.107 filas) mueve el AUC de 0,9298 a 0,9304. Los intervalos se solapan por
-- completo.
-- -----------------------------------------------------------------------------
select train_cutoff, test_period, n_train, n_test,
       round(auc::numeric, 4)         as auc,
       round(auc_ci_low::numeric, 4)  as ci_low,
       round(auc_ci_high::numeric, 4) as ci_high
from predictions.backtest_incremental
order by train_cutoff;
-- 2019_2C  155.141  0,9298  [0,9266; 0,9325]
-- 2020_1C  174.439  0,9300  [0,9268; 0,9328]
-- 2020_2C  192.932  0,9301  [0,9270; 0,9328]
-- 2021_1C  213.107  0,9304  [0,9272; 0,9331]
-- diferencia extremo a extremo: 0,0006


-- -----------------------------------------------------------------------------
-- C7.2  [MATIZAR]  La partición agrupada por legajo
--
-- TEXTO: "Una partición agrupada por legajo mostró que el desempeño sobre
--         estudiantes no observados no se degrada, con un AUC de 0,928 frente al
--         0,930 del backtest estándar."
--
-- El número está bien pero nombra otra cosa. El 0,9277 es el AUC del modelo
-- entrenado con partición agrupada, medido sobre todo el conjunto de prueba.
-- El desempeño sobre los estudiantes que el modelo nunca vio es auc_new, que da
-- 0,9351 y supera al global de 0,9304. La conclusión se refuerza.
-- -----------------------------------------------------------------------------
select test_period, train_cutoff,
       n_test, n_test_seen, n_test_new,
       round(auc_full::numeric, 4)    as auc_full,
       round(auc_seen::numeric, 4)    as auc_vistos,
       round(auc_new::numeric, 4)     as auc_no_vistos,
       n_train_full, n_train_grouped,
       round(auc_grouped::numeric, 4) as auc_particion_agrupada,
       round(auc_grouped_ci_low::numeric, 4)  as ci_low,
       round(auc_grouped_ci_high::numeric, 4) as ci_high
from predictions.backtest_grouped;
-- n_test 23.685 = 13.255 vistos + 10.430 no vistos
-- auc_full 0,9304 | auc_vistos 0,9185 | auc_no_vistos 0,9351
-- auc con partición agrupada 0,9277 [0,9245; 0,9307]


-- -----------------------------------------------------------------------------
-- C7.3  [CORREGIR]  Cobertura real del linaje en el registro de modelos
--
-- Respalda el primer hallazgo del capítulo. Conexión distinta: mlflow_db con el
-- usuario postgres.
--   docker exec -e PGPASSWORD=postgres predun-postgres psql -U postgres -d mlflow_db
-- -----------------------------------------------------------------------------
-- select (select count(*) from model_versions)                                as versiones,
--        (select count(*) from model_version_tags where key='git_commit')     as con_git_commit,
--        (select count(*) from model_version_tags where key='data_version')   as con_data_version;
-- 62 | 4 | 32
