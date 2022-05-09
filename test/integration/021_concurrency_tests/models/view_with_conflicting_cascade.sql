select * from {{ref('table_one')}}

union all

select * from {{ref('table_two')}}
