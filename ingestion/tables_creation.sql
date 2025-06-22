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
    payload->>'ID'                   AS id,
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
    payload->>'nombre_carrera'      AS nombre_carrera,
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
    payload->>'registro_id'          AS registro_id,
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


-- cursada_historica como TEXT
CREATE TABLE IF NOT EXISTS canonical.cursada_historica (
    academic_period TEXT NOT NULL,
    id              TEXT NOT NULL,
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
    inserted_at     TEXT DEFAULT now()::text,
    PRIMARY KEY (id, cod_materia, academic_period)
);

-- alumnos como TEXT
CREATE TABLE IF NOT EXISTS canonical.alumnos (
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
    inserted_at           TEXT DEFAULT now()::text,
    PRIMARY KEY (legajo, academic_period, calidad)
);

-- porcentaje_avance como TEXT
CREATE TABLE IF NOT EXISTS canonical.porcentaje_avance (
    academic_period     TEXT NOT NULL,
    registro_id         TEXT,
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
    inserted_at         TEXT DEFAULT now()::text,
    PRIMARY KEY (registro_id, cod_titulo, academic_period)
);

ALTER TABLE canonical.cursada_historica
ADD COLUMN row_hash TEXT;

UPDATE canonical.cursada_historica
SET row_hash = md5(
    concat_ws('|',
        id, cod_carrera, nom_carrera, anio,
        tipo_cursada, cod_materia, nom_materia, nro_acta,
        origen, nota, fecha, fecha_vigencia, resultado
    )
)
WHERE row_hash IS NULL;

ALTER TABLE canonical.cursada_historica
ADD CONSTRAINT unique_cursada_row UNIQUE (row_hash);

ALTER TABLE canonical.alumnos
ADD COLUMN row_hash TEXT;

UPDATE canonical.alumnos
SET row_hash = md5(
    concat_ws('|',
        legajo, calidad, nombre, plan_nombre, plan_codigo,
        anio_academico, fecha_inscripcion, regular, codigo_carrera, nombre_carrera,
        codigo_pertenece, nombre_pertenece, fecha_nacimiento, nacionalidad,
        pais_nacimiento, sexo, identidad_genero, tipo_ingreso,
        dpto_partido_nombre, localidad_nombre, pais_nombre
    )
)
WHERE row_hash IS NULL;

ALTER TABLE canonical.alumnos
ADD CONSTRAINT unique_alumnos_row UNIQUE (row_hash);

ALTER TABLE canonical.porcentaje_avance
ADD COLUMN row_hash TEXT;

UPDATE canonical.porcentaje_avance
SET row_hash = md5(
    concat_ws('|',
        registro_id, persona_id, es_regular,
        orden_titulo, cod_carrera, nombre_carrera, cod_titulo,
        titulo_obtenido, estado_titulo, reserva_1, reserva_2,
        vigente, porcentaje_avance, materias_aprobadas
    )
)
WHERE row_hash IS NULL;

ALTER TABLE canonical.porcentaje_avance
ADD CONSTRAINT unique_avance_row UNIQUE (row_hash);

ALTER TABLE canonical.cursada_historica DROP CONSTRAINT cursada_historica_pkey;
ALTER TABLE canonical.alumnos DROP CONSTRAINT alumnos_pkey;
ALTER TABLE canonical.porcentaje_avance DROP CONSTRAINT porcentaje_avance_pkey;

grant create, usage on schema public to siu;