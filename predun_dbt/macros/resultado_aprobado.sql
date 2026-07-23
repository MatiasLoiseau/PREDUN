{#
    Vocabulario de resultados académicos de la cursada, centralizado.

    MOTIVO (verificado sobre canonical.cursada_historica, julio 2026):
    en 2021_1C la registración de UNDAV cambió el vocabulario del campo
    `resultado`. Las categorías se intercambiaron por pares y el agregado quedó
    invariante:

        período   Promocionó  Regular  (P+R)   Abandonó  Libre  (A+L)
        2020_2C      44,6 %    15,1 %  59,7 %    31,2 %   7,8 % 38,9 %
        2021_1C      10,8 %    46,1 %  56,9 %     7,8 %  29,4 % 37,2 %
        2025_1C       7,3 %    47,0 %  54,3 %     8,4 %  32,2 % 40,7 %

    No es un cambio de régimen académico: el % de cursadas con nota >= 7 no
    registra ningún quiebre (43,0 → 49,0 → 48,6 → 44,6 entre 2019 y 2025), y la
    nota media de `Regular` sube de 5,82 a 7,46, señal de que el `Regular`
    posterior a 2021 absorbió la población que antes se registraba como
    `Promocionó`.

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
