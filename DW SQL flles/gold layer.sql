

select * from [silver].[crm_cust_info]
select * from [silver].[erp_cust_az12]
select * from [silver].[erp_loc_a101]


create view gold.dim_customers as
select 
row_number() over (order by ci.cst_id) as customer_key,
ci.cst_id as customer_id,
ci.cst_key as customer_number, 
ci.cst_firstname as first_name,
ci.cst_lastname as last_name,
la.cntry as country,
ci.cst_material_status as marital_status,
case when ci.cst_gender != 'n/a' then ci.cst_gender
	else coalesce(ca.gen, 'n/a')
end as gender,
ca.bdate as birth_date,
ci.cst_create_date as create_date
from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca
on ci.cst_key = ca.cid
left join silver.erp_loc_a101 la
on ci.cst_key = la.cid




select * from silver.crm_prd_info

select * from [silver].[erp_px_cat_g1v2]

create view gold.dim_products as
select 
row_number() over (order by prd_id, prd_start_dt) as product_key,
pn.prd_id as product_id,
pn.prd_key as product_number,
pn.prd_nm as product_name,
pn.cat_id as category_id,
pcg.cat as category,
pcg.subcat as sub_category,
pcg.manintenance as maintenance,
pn.prd_cost as cost,
pn.prd_line as product_line,
pn.prd_start_dt as start_date
from silver.crm_prd_info pn
left join silver.erp_px_cat_g1v2 pcg
on pn.cat_id= pcg.id
where prd_end_dt is null




select * from [silver].[crm_sales_details]
drop view gold.fact_sales

create view gold.fact_sales as
select 
sd.sls_ord_num as order_number,
pro.product_key,
cus.customer_key,
sd.sls_order_dt as order_date,
sd.sls_ship_dt as shipping_date,
sd.sls_due_dt as due_date,
sd.sls_sales as sales_amount,
sd.sls_quantity as quantity,
sd.sls_price as price
from silver.crm_sales_details sd
left join gold.dim_products pro
on sd.sls_prd_key= pro.product_number
left join gold.dim_customers cus
on sd.sls_cust_id= cus.customer_id


select * from gold.dim_customers 
select * from gold.dim_products
select * 

select customer_id
from gold.fact_sales
where customer_id not in (select customer_key from gold.dim_customers)