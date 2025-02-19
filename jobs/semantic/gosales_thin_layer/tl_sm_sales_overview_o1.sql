truncate table `{project}.{env}_semantic.tl_sales_overview`;


insert into `{project}.{env}_semantic.tl_sales_overview`
select 
PARSE_DATE('%m/%d/%Y',sale_date) as sale_date,
rd.retailer_name as retailer_name,
rd.retailer_type as retailer_type,
rd.country , 
pl.product as product_name,
pl.product_type as product_type,
pl.product_brand as product_brand,
mh.method_name as method_name,
cast(sum(sf.sell_quantity) as bigint)as sell_quantity,
cast(sum(sf.selling_unit_price) as bigint) as selling_unit_price,
cast(sum(sf.selling_unit_price*sell_quantity) as bigint) as sales_amount,
CURRENT_TIMESTAMP() AS ins_tmstmp, 
CURRENT_TIMESTAMP() AS upd_tmstmp, 
999 as table_id, 
'{batch_id}' AS batch_id,  -- Ensure batch_id is passed correctly
'I' AS oper
from  `{project}.{env}_curated.sales_fact`  sf
left join  `{project}.{env}_curated.retailer_dim` rd ON sf.retailer_key=rd.retailer_key
left join  `{project}.{env}_curated.product_lkp` pl ON sf.product_key=pl.product_key
left join  `{project}.{env}_curated.method_hlp` mh ON sf.method_key=mh.method_key
group by 1,2,3,4,5,6,7,8;