{% macro refresh_canonical() %}
    INSERT INTO canonical.cursada_historica AS tgt
    SELECT * FROM staging.cursada_historica_flat src
    ON CONFLICT (id, cod_materia, academic_period) DO NOTHING;

    INSERT INTO canonical.alumnos AS tgt
    SELECT * FROM staging.alumnos_flat src
    ON CONFLICT (legajo, academic_period) DO NOTHING;

    INSERT INTO canonical.porcentaje_avance AS tgt
    SELECT * FROM staging.porcentaje_avance_flat src
    ON CONFLICT (registro_id, cod_titulo, academic_period) DO NOTHING;
{% endmacro %}