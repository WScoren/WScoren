
use other_db;
set spark.sql.decimalOperations.allowPrecisionLoss=false;
set spark.sql.shuffle.partitions=4;


drop table if exists prem_src1;
create table  prem_src1 as
with t1 as (
    select order_id,
           to_date(event_time) date1
    from tb_order_overall
    where to_date(event_time) between date_sub('2021-10-01',6) and '2021-10-03'
),
     t2 as (
         select date1,
                distinct_pid(td.product_id) over (order by date1 rows between 6 preceding and current row ) as num_pid

         from t1
         join tb_order_detail td on t1.order_id=td.order_id
         join tb_product_info tpi on td.product_id = tpi.product_id
             and shop_id='901'
     ),
     t3 as (
         select date1,
                round(max(num_pid)/(select count(1) from tb_product_info where shop_id='901'),3) sale_rate

         from t2
         where date1 between '2021-10-01' and '2021-10-03'
         group by date1
     ),
     t4 as (
         select *,
               round( 1-sale_rate,3) unsale_rate
         from t3
     )
select *
from t4;

select *
from prem_src1;
