create schema my_schema;

create table my_schema.table1
(field1 int, field2 text)
distributed by (field1);

insert into my_schema.table1
select a, md5(a::text)
from generate_series(1,1000) a;

select gp_segment_id, count(1) from my_schema.table1 group by gp_segment_id;


drop table my_schema.table1;



