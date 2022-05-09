
drop table if exists {schema}.on_model_hook;

create table {schema}.on_model_hook (
    "state"            TEXT, -- start|end

    "target.name"      TEXT,
    "target.schema"    TEXT,
    "target.type"      TEXT,
    "target.threads"   INTEGER,

    "run_started_at"   TEXT,
    "invocation_id"    TEXT
);
