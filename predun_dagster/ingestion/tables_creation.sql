create schema staging;

alter schema staging owner to siu;

create schema canonical;

alter schema canonical owner to siu;

CREATE TABLE IF NOT EXISTS staging.cursada_historica_raw (
    academic_period TEXT NOT NULL,
    payload         JSONB NOT NULL,
    ingested_at     TIMESTAMPTZ DEFAULT now()
) PARTITION BY LIST (academic_period);

CREATE TABLE IF NOT EXISTS staging.alumnos_raw (
    academic_period TEXT NOT NULL,
    payload         JSONB NOT NULL,
    ingested_at     TIMESTAMPTZ DEFAULT now()
) PARTITION BY LIST (academic_period);

CREATE TABLE IF NOT EXISTS staging.porcentaje_avance_raw (
    academic_period TEXT NOT NULL,
    payload         JSONB NOT NULL,
    ingested_at     TIMESTAMPTZ DEFAULT now()
) PARTITION BY LIST (academic_period);

CREATE OR REPLACE VIEW staging.cursada_historica_flat AS
SELECT
    academic_period,
    payload->>'legajo'               AS legajo,
    payload->>'COD_CARRERA'          AS cod_carrera,
    payload->>'NOM_CARRERA'          AS nom_carrera,
    payload->>'ANIO'                 AS anio,
    payload->>'TIPO_CURSADA'         AS tipo_cursada,
    payload->>'COD_MATERIA'          AS cod_materia,
    payload->>'NOM_MATERIA'          AS nom_materia,
    payload->>'NRO_ACTA'             AS nro_acta,
    payload->>'ORIGEN'               AS origen,
    payload->>'NOTA'                 AS nota,
    payload->>'FECHA'                AS fecha,
    payload->>'FECHA_VIGENCIA'       AS fecha_vigencia,
    payload->>'RESULTADO'            AS resultado
FROM staging.cursada_historica_raw;

CREATE OR REPLACE VIEW staging.alumnos_flat AS
SELECT
    academic_period,
    payload->>'legajo'               AS legajo,
    payload->>'calidad'              AS calidad,
    payload->>'nombre'               AS nombre,
    payload->>'plan_nombre'          AS plan_nombre,
    payload->>'plan_codigo'          AS plan_codigo,
    payload->>'anio_academico'       AS anio_academico,
    payload->>'fecha_inscripcion'    AS fecha_inscripcion,
    payload->>'regular'              AS regular,
    payload->>'codigo_carrera'       AS codigo_carrera,
    payload->>'nombre_carrera'       AS nombre_carrera,
    payload->>'codigo_pertenece'     AS codigo_pertenece,
    payload->>'nombre_pertenece'     AS nombre_pertenece,
    payload->>'fecha_nacimiento'     AS fecha_nacimiento,
    payload->>'nacionalidad'         AS nacionalidad,
    payload->>'pais_nacimiento'      AS pais_nacimiento,
    payload->>'sexo'                 AS sexo,
    payload->>'identidad_genero'     AS identidad_genero,
    payload->>'tipo_ingreso'         AS tipo_ingreso,
    payload->>'dpto_partido_nombre'  AS dpto_partido_nombre,
    payload->>'localidad_nombre'     AS localidad_nombre,
    payload->>'pais_nombre'          AS pais_nombre
FROM staging.alumnos_raw;

CREATE OR REPLACE VIEW staging.porcentaje_avance_flat AS
SELECT
    academic_period,
    payload->>'legajo'               AS legajo,
    payload->>'persona_id'           AS persona_id,
    payload->>'es_regular'           AS es_regular,
    payload->>'orden_titulo'         AS orden_titulo,
    payload->>'cod_carrera'          AS cod_carrera,
    payload->>'nombre_carrera'       AS nombre_carrera,
    payload->>'cod_titulo'           AS cod_titulo,
    payload->>'titulo_obtenido'      AS titulo_obtenido,
    payload->>'estado_titulo'        AS estado_titulo,
    payload->>'reserva_1'            AS reserva_1,
    payload->>'reserva_2'            AS reserva_2,
    payload->>'vigente'              AS vigente,
    payload->>'porcentaje_avance'    AS porcentaje_avance,
    payload->>'materias_aprobadas'   AS materias_aprobadas
FROM staging.porcentaje_avance_raw;
select * from staging.porcentaje_avance_raw;

CREATE TABLE IF NOT EXISTS canonical.cursada_historica (
    row_hash        TEXT PRIMARY KEY,
    academic_period TEXT NOT NULL,
    legajo          TEXT,
    cod_carrera     TEXT,
    nom_carrera     TEXT,
    anio            TEXT,
    tipo_cursada    TEXT,
    cod_materia     TEXT,
    nom_materia     TEXT,
    nro_acta        TEXT,
    origen          TEXT,
    nota            TEXT,
    fecha           TEXT,
    fecha_vigencia  TEXT,
    resultado       TEXT,
    inserted_at     TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS canonical.alumnos (
    row_hash              TEXT PRIMARY KEY,
    academic_period       TEXT NOT NULL,
    legajo                TEXT,
    calidad               TEXT,
    nombre                TEXT,
    plan_nombre           TEXT,
    plan_codigo           TEXT,
    anio_academico        TEXT,
    fecha_inscripcion     TEXT,
    regular               TEXT,
    codigo_carrera        TEXT,
    nombre_carrera        TEXT,
    codigo_pertenece      TEXT,
    nombre_pertenece      TEXT,
    fecha_nacimiento      TEXT,
    nacionalidad          TEXT,
    pais_nacimiento       TEXT,
    sexo                  TEXT,
    identidad_genero      TEXT,
    tipo_ingreso          TEXT,
    dpto_partido_nombre   TEXT,
    localidad_nombre      TEXT,
    pais_nombre           TEXT,
    inserted_at           TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS canonical.porcentaje_avance (
    row_hash          TEXT PRIMARY KEY,
    academic_period   TEXT NOT NULL,
    legajo            TEXT,
    persona_id        TEXT,
    es_regular        TEXT,
    orden_titulo      TEXT,
    cod_carrera       TEXT,
    nombre_carrera    TEXT,
    cod_titulo        TEXT,
    titulo_obtenido   TEXT,
    estado_titulo     TEXT,
    reserva_1         TEXT,
    reserva_2         TEXT,
    vigente           TEXT,
    porcentaje_avance TEXT,
    materias_aprobadas TEXT,
    inserted_at       TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS canonical.cursada_historica_history (
    row_hash        TEXT NOT NULL,
    academic_period TEXT NOT NULL,
    legajo          TEXT,
    cod_carrera     TEXT,
    nom_carrera     TEXT,
    anio            TEXT,
    tipo_cursada    TEXT,
    cod_materia     TEXT,
    nom_materia     TEXT,
    nro_acta        TEXT,
    origen          TEXT,
    nota            TEXT,
    fecha           TEXT,
    fecha_vigencia  TEXT,
    resultado       TEXT,
    inserted_at     TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (row_hash, academic_period)
);

CREATE TABLE IF NOT EXISTS canonical.alumnos_history (
    row_hash              TEXT NOT NULL,
    academic_period       TEXT NOT NULL,
    legajo                TEXT,
    calidad               TEXT,
    nombre                TEXT,
    plan_nombre           TEXT,
    plan_codigo           TEXT,
    anio_academico        TEXT,
    fecha_inscripcion     TEXT,
    regular               TEXT,
    codigo_carrera        TEXT,
    nombre_carrera        TEXT,
    codigo_pertenece      TEXT,
    nombre_pertenece      TEXT,
    fecha_nacimiento      TEXT,
    nacionalidad          TEXT,
    pais_nacimiento       TEXT,
    sexo                  TEXT,
    identidad_genero      TEXT,
    tipo_ingreso          TEXT,
    dpto_partido_nombre   TEXT,
    localidad_nombre      TEXT,
    pais_nombre           TEXT,
    inserted_at           TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (row_hash, academic_period)
);

CREATE TABLE IF NOT EXISTS canonical.porcentaje_avance_history (
    row_hash            TEXT NOT NULL,
    academic_period     TEXT NOT NULL,
    legajo              TEXT,
    persona_id          TEXT,
    es_regular          TEXT,
    orden_titulo        TEXT,
    cod_carrera         TEXT,
    nombre_carrera      TEXT,
    cod_titulo          TEXT,
    titulo_obtenido     TEXT,
    estado_titulo       TEXT,
    reserva_1           TEXT,
    reserva_2           TEXT,
    vigente             TEXT,
    porcentaje_avance   TEXT,
    materias_aprobadas  TEXT,
    inserted_at         TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (row_hash, academic_period)
);

CREATE SCHEMA IF NOT EXISTS marts;

alter schema marts owner to siu;

CREATE TABLE IF NOT EXISTS marts.student_status (
    legajo               TEXT PRIMARY KEY,
    status               TEXT      NOT NULL,
    dropout_flag         INTEGER   NOT NULL,
    fecha_ultima_cursada DATE,
    inserted_at          TIMESTAMPTZ DEFAULT now()
);

create table marts.student_panel (
    legajo                    integer          not null,
    cod_carrera               varchar(10)      not null,
    academic_period           varchar(8)       not null,
    materias_en_periodo       integer          not null,
    promo_en_periodo          integer          not null,
    nota_media_en_periodo     numeric(5,2),
    materias_win3             integer          not null,
    promo_win3                integer          not null,
    nota_win3                 numeric(5,2),
    dias_desde_ult_periodo    integer,
    dropout_next              integer          not null
);