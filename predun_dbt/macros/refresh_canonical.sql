{% macro refresh_canonical() %}
    {{ log("Insertando cursada_historica", info=True) }}
    {{ run_query("""
        INSERT INTO canonical.cursada_historica (
            academic_period,
            id,
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
            row_hash
        )
        SELECT
            academic_period,
            id,
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
            md5(concat_ws('|',
                academic_period, id, cod_carrera, nom_carrera, anio,
                tipo_cursada, cod_materia, nom_materia, nro_acta,
                origen, nota, fecha, fecha_vigencia, resultado
            )) AS row_hash
        FROM staging.cursada_historica_flat
        WHERE id IS NOT NULL AND cod_materia IS NOT NULL AND academic_period IS NOT NULL
        ON CONFLICT ON CONSTRAINT unique_cursada_row DO NOTHING;
    """) }}

    {{ log("Insertando alumnos", info=True) }}
    {{ run_query("""
        INSERT INTO canonical.alumnos (
            academic_period,
            legajo,
            calidad,
            nombre,
            plan_nombre,
            plan_codigo,
            anio_academico,
            fecha_inscripcion,
            regular,
            codigo_carrera,
            nombre_carrera,
            codigo_pertenece,
            nombre_pertenece,
            fecha_nacimiento,
            nacionalidad,
            pais_nacimiento,
            sexo,
            identidad_genero,
            tipo_ingreso,
            dpto_partido_nombre,
            localidad_nombre,
            pais_nombre,
            row_hash
        )
        SELECT
            academic_period,
            legajo,
            calidad,
            nombre,
            plan_nombre,
            plan_codigo,
            anio_academico,
            fecha_inscripcion,
            regular,
            codigo_carrera,
            nombre_carrera,
            codigo_pertenece,
            nombre_pertenece,
            fecha_nacimiento,
            nacionalidad,
            pais_nacimiento,
            sexo,
            identidad_genero,
            tipo_ingreso,
            dpto_partido_nombre,
            localidad_nombre,
            pais_nombre,
            md5(concat_ws('|',
                academic_period, legajo, calidad, nombre, plan_nombre,
                plan_codigo, anio_academico, fecha_inscripcion, regular,
                codigo_carrera, nombre_carrera, codigo_pertenece, nombre_pertenece,
                fecha_nacimiento, nacionalidad, pais_nacimiento, sexo,
                identidad_genero, tipo_ingreso, dpto_partido_nombre,
                localidad_nombre, pais_nombre
            )) AS row_hash
        FROM staging.alumnos_flat
        WHERE legajo IS NOT NULL AND academic_period IS NOT NULL
        ON CONFLICT ON CONSTRAINT unique_alumnos_row DO NOTHING;
    """) }}

    {{ log("Insertando porcentaje_avance", info=True) }}
    {{ run_query("""
        INSERT INTO canonical.porcentaje_avance (
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
            row_hash
        )
        SELECT
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
            md5(concat_ws('|',
                academic_period, registro_id, persona_id, es_regular,
                orden_titulo, cod_carrera, nombre_carrera, cod_titulo,
                titulo_obtenido, estado_titulo, reserva_1, reserva_2,
                vigente, porcentaje_avance, materias_aprobadas
            )) AS row_hash
        FROM staging.porcentaje_avance_flat
        WHERE registro_id IS NOT NULL AND cod_titulo IS NOT NULL AND academic_period IS NOT NULL
        ON CONFLICT ON CONSTRAINT unique_avance_row DO NOTHING;
    """) }}

    {% set inserted_rows = run_query("SELECT COUNT(*) FROM canonical.cursada_historica") %}
    {{ log("Filas en canonical.cursada_historica después de insertar: " ~ inserted_rows.columns[0].values()[0], info=True) }}
{% endmacro %}