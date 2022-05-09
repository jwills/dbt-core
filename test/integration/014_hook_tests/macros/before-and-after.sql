
{% macro custom_run_hook(state, target, run_started_at, invocation_id) %}

   insert into {{ target.schema }}.on_run_hook (
        "state",
        "target.name",
        "target.schema",
        "target.type",
        "target.threads",
        "run_started_at",
        "invocation_id"
   ) VALUES (
    '{{ state }}',
    '{{ target.name }}',
    '{{ target.schema }}',
    '{{ target.type }}',
    {{ target.threads }},
    '{{ run_started_at }}',
    '{{ invocation_id }}'
   )

{% endmacro %}
