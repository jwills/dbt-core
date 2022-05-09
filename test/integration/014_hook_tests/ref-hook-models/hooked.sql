{{
    config({
        "pre_hook": "\
            insert into {{this.schema}}.on_model_hook select
                state,
                '{{ target.name }}' as \"target.name\",\
                '{{ target.schema }}' as \"target.schema\",\
                '{{ target.type }}' as \"target.type\",\
                {{ target.threads }} as \"target.threads\",\
                '{{ run_started_at }}' as \"run_started_at\",\
                '{{ invocation_id }}' as \"invocation_id\"\
                from {{ ref('pre') }}\
        "
    })
}}
select 1 as id
