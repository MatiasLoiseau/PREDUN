{#
    Vocabulario de resultados académicos de la cursada, centralizado.

    MOTIVO (verificado sobre canonical.cursada_historica, julio 2026):
    en 2021_1C hay un quiebre en el campo `resultado`. Las categorías se
    intercambian por pares (Promocionó↔Regular y Abandonó↔Libre) y el agregado
    P+R queda estable a ambos lados del corte. Las cifras están en la tabla de
    vocabulario del Cap. 4 de la tesis, agrupadas por fecha del acta. La causa
    no está confirmada por la institución y acá no se infiere ninguna.

    Consecuencia: contar solo 'Promocionó' produce una feature no comparable a
    ambos lados de 2021_1C — que es justamente el corte de entrenamiento. El
    predicado invariante al régimen es "aprobó la cursada" = Promocionó ∪ Regular.

    Adaptación a otra institución: redefinir este macro alcanza; ningún modelo
    aguas abajo referencia literales de `resultado`.
#}

{% macro resultado_aprobado(col='resultado') %}
    ({{ col }} ilike 'Promoc%' or {{ col }} = 'Regular')
{% endmacro %}


{#
    Cursadas no aprobadas. Complemento explícito del anterior; se define aparte
    para que el vocabulario quede en un solo lugar aunque hoy no se use como
    feature. Mismo intercambio por pares: 'Abandonó' pre-2021 ≡ 'Libre' post-2021.
#}

{% macro resultado_no_aprobado(col='resultado') %}
    ({{ col }} in ('Abandonó', 'Libre', 'Insuficiente', 'No Promocionó'))
{% endmacro %}
