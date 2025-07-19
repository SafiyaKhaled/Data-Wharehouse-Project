
-- create datahwarehouse
use master;
go

-- drop and recreate the datawhaerhosue database

if exists (select 1 from sys.databases where name= 'datawharehosue')
begin
	alter database datawharehosue set single_user with rollback immediate;
	drop database datawharehouse;
end;
go


create database datawharehouse;
go

use datawharehouse;
go

-- Create the schemas
create schema bronze;
go
create schema silver;
go
create schema gold;
go

-- create data definition language of bronze layer

if object_id ('bronze.crm_cust_info', 'U') is not null
drop table bronze.crm_cust_info;

create table bronze.crm_cust_info (
cst_id int,
cst_key nvarchar (50),
cst_firstname nvarchar (50),
cst_lastname  nvarchar (50),
cst_material_status nvarchar (50),
cst_gender  nvarchar (50),
cst_create_date date
)

----------------------------
if object_id ('bronze.crm_prd_info', 'U') is not null
drop table bronze.crm_prd_info;

create table bronze.crm_prd_info (
prd_id int,
prd_key nvarchar (50),
prd_nm nvarchar (50),
prd_cost  int,
prd_line nvarchar (50),
prd_start_dt  datetime,
prd_end_dt datetime
)

----------------------------------
if object_id ('bronze.crm_sales_details', 'U') is not null
drop table bronze.crm_sales_details;

create table bronze.crm_sales_details (
sls_ord_num nvarchar (50),
sls_prd_key nvarchar (50),
sls_cust_id int,
sls_order_dt  int,
sls_ship_dt int,
sls_due_dt int,
sls_sales int,
sls_quantity int,
sls_price int
)
-------------------------------------------
if object_id ('bronze.erp_loc_a101', 'U') is not null
drop table bronze.erp_loc_a101;

create table bronze.erp_loc_a101 (
cid nvarchar (50),
cntry nvarchar (50)
)
-----------------------------------------
if object_id ('bronze.erp_cust_az12', 'U') is not null
drop table bronze.erp_cust_az12;

create table bronze.erp_cust_az12 (
cid nvarchar (50),
bdate date,
gen nvarchar (50)
)
----------------------------------------------
if object_id ('bronze.erp_px_cat_g1v2', 'U') is not null
drop table bronze.erp_px_cat_g1v2;

create table bronze.erp_px_cat_g1v2 (
id nvarchar (50),
cat nvarchar (50),
subcat nvarchar (50),
manintenance nvarchar (50)
)
--------------------------------
-- creating the bronze layer database

create or alter procedure bronze.load_bronze as 
begin
	Declare @start_time datetime, @end_time datetime;
begin try 
	print '====================================================';
	print 'Loading Bronze Layer';
	print '====================================================';

	print '-----------------------------------------------------';
	print 'Loading CRM Tables';
	print '-----------------------------------------------------';
	
	set @start_time= getdate();
	print '>> Truncating Table: bronze.crm_cust_info'
	Truncate table bronze.crm_cust_info

	print '>> Inserting Data into: bronze.crm_cust_info'
	bulk insert bronze.crm_cust_info
	from 'C:\Users\HP\Desktop\SQL\Datawharehouse project\datasets\source_crm\cust_info.csv'
	with(
	firstrow= 2, fieldterminator= ',', tablock
	);
	set @end_time = getdate();
	print '>> load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'Seconds'
	print '---------------------------------------' 

	set @start_time = getdate();
	print '>> Truncating Table: bronze.crm_prd_info'
	
	Truncate table bronze.crm_prd_info

	print '>> Inserting Data into: bronze.crm_prd_info'
	bulk insert bronze.crm_prd_info
	from 'C:\Users\HP\Desktop\SQL\Datawharehouse project\datasets\source_crm\prd_info.csv'
	with(
	firstrow= 2, fieldterminator= ',', tablock
	);
	set @end_time = getdate();
	print '>> load duration: ' + cast(datediff(second,@start_time, @end_time) as nvarchar) + 'Seconds'
	print '---------------------------------------' 

	set @start_time = getdate();
	print '>> Truncating Table: bronze.crm_sales_details'
	Truncate table bronze.crm_sales_details

	print '>> Inserting Data into: bronze.crm_sales_details'
	bulk insert bronze.crm_sales_details
	from 'C:\Users\HP\Desktop\SQL\Datawharehouse project\datasets\source_crm\sales_details.csv'
	with(
	firstrow= 2, fieldterminator= ',', tablock
	);
	set @end_time = getdate();
	print '>> load duration: ' + cast(datediff(second,@start_time, @end_time) as nvarchar) + 'Seconds'
	print '---------------------------------------' 

	set @start_time = getdate();
	print '-----------------------------------------------------';
	print 'Loading ERP Tables';
	print '-----------------------------------------------------';

	print '>> Truncating Table: bronze.erp_loc_a101'
	Truncate table bronze.erp_loc_a101

	print '>> Inserting Data into: bronze.erp_loc_a101'
	bulk insert bronze.erp_loc_a101
	from 'C:\Users\HP\Desktop\SQL\Datawharehouse project\datasets\source_erp\LOC_A101.csv'
	with(
	firstrow= 2, fieldterminator= ',', tablock
	);
	set @end_time = getdate();
	print '>> load duration: ' + cast(datediff(second,@start_time, @end_time) as nvarchar) + 'Seconds'
	print '---------------------------------------' 

	set @start_time = getdate();
	print '>> Truncating Table: bronze.erp_cust_az12'
	
	Truncate table bronze.erp_cust_az12

	print '>> Inserting Data into: bronze.erp_cust_az12'
	bulk insert bronze.erp_cust_az12
	from 'C:\Users\HP\Desktop\SQL\Datawharehouse project\datasets\source_erp\CUST_AZ12.csv'
	with(
	firstrow= 2, fieldterminator= ',', tablock
	);
	set @end_time = getdate();
	print '>> load duration: ' + cast(datediff(second,@start_time, @end_time) as nvarchar) + 'Seconds' 
	print '---------------------------------------' 

	set @start_time = getdate();
	print '>> Truncating Table: bronze.erp_px_cat_g1v2'
	Truncate table bronze.erp_px_cat_g1v2

	print '>> Inserting Data into: bronze.erp_px_cat_g1v2'
	bulk insert bronze.erp_px_cat_g1v2
	from 'C:\Users\HP\Desktop\SQL\Datawharehouse project\datasets\source_erp\PX_CAT_G1V2.csv'
	with(
	firstrow= 2, fieldterminator= ',', tablock
	);
	set @end_time = getdate();
	print '>> load duration: ' + cast(datediff(second,@start_time, @end_time) as nvarchar) + 'Seconds'
	print '---------------------------------------' 

end try
begin catch
print '============================================'
print 'error occured during loading bronze layer'
print 'error message' + Error_message();
print 'error message' + cast(Error_number() as nvarchar);
print 'error message' + cast(Error_state() as nvarchar);
print '============================================'
end catch
end

exec  bronze.load_bronze



