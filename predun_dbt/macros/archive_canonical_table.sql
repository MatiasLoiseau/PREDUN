{#
    Archiva la versión vigente de una tabla canonical antes de rematerializarla.
    Es el mecanismo de versionado de datos del sistema: permite reproducir un
    entrenamiento pasado recuperando la tabla canónica de ese momento.

    Robusto ante dos situaciones que la versión anterior no contemplaba:

    (1) La tabla todavía no existe (primera materialización sobre una base
        vacía). Antes se intentaba `create table _history (like {{ this }})`
        contra una relación inexistente. Ahora simplemente no hay nada que
        archivar y el hook no hace nada.

    (2) El esquema de la tabla cambió respecto del histórico acumulado. Antes el
        `insert ... select *` era POSICIONAL, así que al agregar o reordenar
        columnas fallaba con "INSERT has more expressions than target columns".
        Concretamente: la corrección de julio 2026 sumó `evento_hash`, `n_actas`
        y `origenes` a `cursada_historica` (16 -> 19 columnas), lo que habría
        roto el segundo ciclo del reproceso. Ahora, si los conjuntos de columnas
        difieren, el histórico se ROTA (se renombra con marca temporal) y se crea
        uno nuevo con el esquema vigente. No se pierde nada: las versiones
        anteriores quedan en la tabla rotada.

    Además el insert pasa a ser por NOMBRE de columna, no por posición, de modo
    que un reordenamiento futuro deje de ser un problema.
#}

{% macro archive_canonical_table(this) %}

  {% if execute %}

    {% set src = adapter.get_relation(
           database=this.database,
           schema=this.schema,
           identifier=this.identifier) %}

    {# (1) Nada que archivar: la tabla aún no existe. #}
    {% if src is not none %}

      {% set history_identifier = this.identifier ~ '_history' %}
      {% set history_relation = adapter.get_relation(
             database=this.database,
             schema=this.schema,
             identifier=history_identifier) %}

      {% set src_cols = adapter.get_columns_in_relation(src)
                        | map(attribute='name') | list %}

      {# (2) Rotación por cambio de esquema. #}
      {% if history_relation is not none %}
        {% set hist_cols = adapter.get_columns_in_relation(history_relation)
                           | map(attribute='name') | list %}
        {% if src_cols | sort != hist_cols | sort %}
          {% set stamp = run_started_at.strftime('%Y%m%d%H%M%S') %}
          alter table {{ this.schema }}.{{ history_identifier }}
              rename to {{ history_identifier }}_pre_{{ stamp }};
          {% set history_relation = none %}
        {% endif %}
      {% endif %}

      {% if history_relation is none %}
        create table {{ this.schema }}.{{ history_identifier }}
        (
            like {{ src }}
            including defaults
            including comments
            including storage
        );
        alter table {{ this.schema }}.{{ history_identifier }}
            add primary key (row_hash, academic_period);
      {% endif %}

      {% set col_list = src_cols | join(', ') %}
      insert into {{ this.schema }}.{{ history_identifier }} ({{ col_list }})
      select {{ col_list }}
      from {{ src }}
      on conflict (row_hash, academic_period)
      do nothing;

    {% endif %}

  {% endif %}

{% endmacro %}
