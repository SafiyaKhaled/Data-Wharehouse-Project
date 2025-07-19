-- creating the silver layer tables

if object_id ('silver.crm_cust_info', 'U') is not null
drop table silver.crm_cust_info;

create table silver.crm_cust_info (
cst_id int,
cst_key nvarchar (50),
cst_firstname nvarchar (50),
cst_lastname  nvarchar (50),
cst_material_status nvarchar (50),
cst_gender  nvarchar (50),
cst_create_date date,
dwh_create_date datetime2 default getdate()
);

----------------------------
if object_id ('silver.crm_prd_info', 'U') is not null
drop table silver.crm_prd_info;

create table silver.crm_prd_info (
prd_id int,
cat_id nvarchar (50),
prd_key nvarchar (50),
prd_nm nvarchar (50),
prd_cost  int,
prd_line nvarchar (50),
prd_start_dt  date,
prd_end_dt date,
dwh_create_date datetime2 default getdate()
)

----------------------------------
if object_id ('silver.crm_sales_details', 'U') is not null
drop table silver.crm_sales_details;

create table silver.crm_sales_details (
sls_ord_num nvarchar (50),
sls_prd_key nvarchar (50),
sls_cust_id int,
sls_order_dt  date,
sls_ship_dt date,
sls_due_dt date,
sls_sales int,
sls_quantity int,
sls_price int,
dwh_create_date datetime2 default getdate()
)
-------------------------------------------
create table silver.erp_loc_a101 (
cid nvarchar (50),
cntry nvarchar (50),
dwh_create_date datetime2 default getdate()
)
-----------------------------------------
create table silver.erp_cust_az12 (
cid nvarchar (50),
bdate date,
gen nvarchar (50),
dwh_create_date datetime2 default getdate()

)
----------------------------------------------
create table silver.erp_px_cat_g1v2 (
id nvarchar (50),
cat nvarchar (50),
subcat nvarchar (50),
manintenance nvarchar (50),
dwh_create_date datetime2 default getdate()
)


-- check for nulls or duplicates in primary key
-- expectation: no result
-- check for spaces after and before the string values

select * from bronze.crm_cust_info

select cst_id, count(*)
from bronze.crm_cust_info
group by cst_id
having count(*)>1 or cst_id is null

-----------------------------------------------

insert into silver.crm_cust_info (
cst_id,
cst_key,
cst_firstname,
cst_lastname,
cst_material_status,
cst_gender,
cst_create_date
)
select 
cst_id,
cst_key,
trim(cst_firstname) as cst_firstname,
trim(cst_lastname) as cst_lastname,
case when upper(trim(cst_material_status)) = 'S' then 'Single'
	 when upper(trim(cst_material_status)) = 'M' then 'Married'
	 else 'n/a' 
end cst_material_status,
case when upper(trim(cst_gender)) = 'F' then 'Female'
	 when upper(trim(cst_gender)) = 'M' then 'Male'
	 else 'n/a' 
end cst_gender,
cst_create_date
from(
select *, row_number() over (partition by cst_id order by cst_create_date desc)as flag_last
from bronze.crm_cust_info) t
where flag_last =1 AND cst_id is not null

 -- data standardization & consistency
 select distinct cst_gender
 from silver.crm_cust_info

  select distinct cst_material_status
 from silver.crm_cust_info
-----------------------------------------------------------------
---------
-----------------------------------------------------------------

 select * 
 from bronze.crm_prd_info

 select prd_id, count(*)
from bronze.crm_prd_info
group by prd_id
having count(*)>1 or prd_id is null


insert into silver.crm_prd_info
(prd_id,
cat_id,
prd_key,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt)
select 
prd_id,
replace(substring(prd_key,1,5), '-', '_') as cat_id,
substring(prd_key, 7, len(prd_key)) as prd_key,
prd_nm,
isnull(prd_cost,0) as prd_cost,
case when upper(trim(prd_line)) = 'M' then 'Mountain'
	 when upper(trim(prd_line)) = 'R' then 'Road'
	 when upper(trim(prd_line)) = 'S' then 'Other Sales'
	 when upper(trim(prd_line)) = 'T' then 'Touring'
	 else 'n/a' 
end prd_line,
cast(prd_start_dt as date) as prd_start_dt, 
cast(lead(prd_start_dt)over (partition by prd_key order by prd_start_dt)-1 as date) as prd_end_dt
 from bronze.crm_prd_info


 -- check for unwanted spaces
 select prd_nm
 from silver.crm_prd_info
 where prd_nm != trim(prd_nm)

 -- check fro nulls or negative numbers
 select prd_cost
 from silver.crm_prd_info
 where prd_cost< 0 or prd_cost is null
  -- data standadrization & consistency
   select distinct prd_line
 from silver.crm_prd_info

 -- check for invalid date orders 
 select * from silver.crm_prd_info
 where prd_end_dt < prd_start_dt


-----------------------------------------------------------------
---------
-----------------------------------------------------------------

 select *
 from bronze.crm_sales_details

 select sls_ord_num, count(*)
 from bronze.crm_sales_details
 group by sls_ord_num
 having count(*) > 1

 select * from bronze.crm_sales_details 
 where sls_ord_num = 'SO54299'

 -- check for unwanted spaces
 select sls_ord_num
 from bronze.crm_sales_details
 where sls_ord_num != trim(sls_ord_num)

  -- check fro nulls or negative numbers
 select *
 from bronze.crm_sales_details
 where sls_sales< 0 or sls_sales is null

  -- check for invalid date orders 
 select * from bronze.crm_sales_details
 where sls_ship_dt < sls_order_dt

 -- check integrety of product key
 select * from bronze.crm_sales_details
 where sls_prd_key not in (select prd_key from silver.crm_prd_info)

  -- check integrety of custoemr key
  select * from bronze.crm_sales_details
 where sls_cust_id not in (select cst_id from silver.crm_cust_info)

 -- check for invalid dates
select * from bronze.crm_sales_details
 where sls_ship_dt <=0 or sls_due_dt <=0 or sls_order_dt <=0 or len(sls_order_dt) !=8

-- check valid of sls_sales and price
select 
sls_prd_key,
sls_sales, 
sls_quantity ,
sls_price
from  bronze.crm_sales_details
where sls_sales != sls_quantity * sls_price
or sls_sales is null or sls_quantity is null or sls_price is null
or sls_sales <=0 or sls_quantity <=0 or sls_price <=0


--show if the product has the same price or not 
select sls_prd_key, sls_price, dense_rank() over (partition by sls_prd_key order by sls_price) as RN
from  bronze.crm_sales_details
where sls_price is not null

insert into silver.crm_sales_details (
	sls_ord_num,
	sls_prd_key ,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price)
select
 sls_ord_num,
 sls_prd_key,
 sls_cust_id,
case when sls_order_dt = 0 or len(sls_order_dt)!=8 then null
	Else cast(cast(sls_order_dt as varchar) as date)
 end as sls_order_dt,
 case when sls_ship_dt = 0 or len(sls_ship_dt)!=8 then null
	Else cast(cast(sls_ship_dt as varchar) as date)
 end as sls_ship_dt,
 case when sls_due_dt = 0 or len(sls_due_dt)!=8 then null
	Else cast(cast(sls_due_dt as varchar) as date)
 end as sls_due_dt,
case when sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity * abs(sls_price)
	then sls_quantity * abs(sls_price)
	else sls_sales
end as sls_sales,
sls_quantity,
case when sls_price is null or sls_price <=0 
	then abs(sls_sales)/ nullif(sls_quantity,0) 
	else sls_price
end as sls_price
from  bronze.crm_sales_details

select * from silver.crm_sales_details
-----------------------------------------------------------------
---------
-----------------------------------------------------------------

-- check all custoemr id is in the silver.crm_cust_info table
select 
case when cid like 'NAS%' then substring(cid, 4, len(cid))
else cid
end as cid,
bdate,
gen
from bronze.erp_cust_az12
where case when cid like 'NAS%' then substring(cid, 4, len(cid))
else cid
end not in (select distinct cst_key from silver.crm_cust_info)


-- identify out of range dates
select 
bdate
from bronze.erp_cust_az12
where bdate > getdate()

-- data standadrization and consistency 
select distinct gen
from bronze.erp_cust_az12
-------------------------------------------
insert into silver.erp_cust_az12(
		cid,
		bdate,
		gen
)
select 
case when cid like 'NAS%' then substring(cid, 4, len(cid))
else cid
end as cid,
case when bdate > getdate()
	THEN NULL 
	ELSE bdate
END AS bdate,
case when gen = 'F' then 'Female'
	when gen = 'M' then 'Male'
	when gen = '' then 'N/A'
	when gen is null then 'N/A'
	else gen
end as gen
from bronze.erp_cust_az12

select * from silver.erp_cust_az12
-----------------------------------------------------------------
---------
-----------------------------------------------------------------

select cid,
cntry
from bronze.erp_loc_a101

-- check all custoemr id is in the silver.crm_cust_info table
select replace(cid, '-', '') cid,
cntry
from bronze.erp_loc_a101
where replace(cid, '-', '') not in (select cst_key from silver.crm_cust_info)

select * from silver.crm_cust_info

-- data standardzation and consistency
select distinct cntry,
case when trim(cntry) = 'DE' then 'Germany'
	when trim(cntry) in ('USA', 'US') then 'United States'
	when trim(cntry) = '' then 'n/a'
	when trim(cntry) is null then 'n/a'
	else cntry
end cntry
from bronze.erp_loc_a101



--------------------------------------------
insert into silver.erp_loc_a101(
		cid,
		cntry
)
select replace(cid, '-', '') cid,
case when trim(cntry) = 'DE' then 'Germany'
	when trim(cntry) in ('USA', 'US') then 'United States'
	when trim(cntry) = '' then 'n/a'
	when trim(cntry) is null then 'n/a'
	else cntry
end cntry
from bronze.erp_loc_a101

select * from silver.erp_loc_a101


-----------------------------------------------------------------
---------
-----------------------------------------------------------------
select * from bronze.erp_px_cat_g1v2
select * from silver.crm_prd_info


-- check unwanted spaces
select * 
from bronze.erp_px_cat_g1v2
where cat != trim(cat) or subcat != trim(subcat) or manintenance != trim(manintenance)

-- -- check all product category id is in the silver.crm_prd_info
select trim(id) as id
from bronze.erp_px_cat_g1v2
where trim(id) not in (select cat_id from silver.crm_prd_info)

-- check data standardzation and consistency
select distinct cat
from bronze.erp_px_cat_g1v2

select distinct subcat
from bronze.erp_px_cat_g1v2

select distinct manintenance
from bronze.erp_px_cat_g1v2
------------------------------------
insert into silver.erp_px_cat_g1v2(
		id,
		cat,
		subcat,
		manintenance
)
select id,
cat, 
subcat, 
manintenance
from bronze.erp_px_cat_g1v2

select * from silver.erp_px_cat_g1v2