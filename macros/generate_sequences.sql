{% macro generate_sequences() %}

    {% if execute %}

        {# Find models with the meta surrogate key configuration #}
        {% set models = graph.nodes.values() | selectattr('resource_type', 'eq', 'model') %}
        {% set skey_models = [] %}
        {% for model in models %}
            {% if model.config.meta.surrogate_key %}
                {% do skey_models.append(model) %}
            {% endif %}
        {% endfor %}

    {% endif %}

    {# Generate sequences for models with surrogate key configuration #}
    {% for model in skey_models %}

        {% if flags.FULL_REFRESH or model.config.materialized == 'table' %}
            {# Regenerate sequences during full refresh and when recreating table #}
            create or replace sequence {{ model.database }}.{{ model.schema }}.{{ model.name }}_seq;

        {% else %}
            {# Create only if not exists for incremental models #}
            create sequence if not exists {{ model.database }}.{{ model.schema }}.{{ model.name }}_seq;
        {% endif %}

    {% endfor %}
{% endmacro %}
