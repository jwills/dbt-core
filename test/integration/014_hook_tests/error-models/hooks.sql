
{{
    config({
        "pre_hook": "\
            insert into {{this.schema}}.on_model_hook (\
                \"state\",\
                \"target.name\",\
                \"target.schema\",\
                \"target.type\",\
                \"target.threads\",\
                \"run_started_at\",\
                \"invocation_id\"\
            ) VALUES (\
                'start',\
                '{{ target.name }}',\
                '{{ target.schema }}',\
                '{{ target.type }}',\
                {{ target.threads }},\
                '{{ run_started_at }}',\
                '{{ invocation_id }}'\
        )",
        "pre-hook": "\
            insert into {{this.schema}}.on_model_hook (\
                \"state\",\
                \"target.name\",\
                \"target.schema\",\
                \"target.type\",\
                \"target.threads\",\
                \"run_started_at\",\
                \"invocation_id\"\
            ) VALUES (\
                'start',\
                '{{ target.name }}',\
                '{{ target.schema }}',\
                '{{ target.type }}',\
                {{ target.threads }},\
                '{{ run_started_at }}',\
                '{{ invocation_id }}'\
        )",
        "post-hook": "\
            insert into {{this.schema}}.on_model_hook (\
                \"state\",\
                \"target.name\",\
                \"target.schema\",\
                \"target.type\",\
                \"target.threads\",\
                \"run_started_at\",\
                \"invocation_id\"\
            ) VALUES (\
                'end',\
                '{{ target.name }}',\
                '{{ target.schema }}',\
                '{{ target.type }}',\
                {{ target.port }},\
                {{ target.threads }},\
                '{{ run_started_at }}',\
                '{{ invocation_id }}'\
            )"
    })
}}

select 3 as id
