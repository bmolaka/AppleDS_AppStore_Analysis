/* This script pulls the data from multiple csv files and load them to sql tables. 
Then, all the datasets are combined together for further analysis. 
*/

-- Load the transaction data into temp table
LOAD DATA LOCAL INFILE '\Users\bmolaka\Desktop\Apple-DS\transaction_dat.csv'
INTO TABLE test.dummy_transaction_dat FIELDS TERMINATED BY ','
ENCLOSED BY '"' LINES TERMINATED BY '\n';

-- Load the account data into temp table
LOAD DATA LOCAL INFILE '\Users\bmolaka\Desktop\Apple-DS\account_dat.csv'
INTO TABLE test.dummy_account_dat FIELDS TERMINATED BY ','
ENCLOSED BY '"' LINES TERMINATED BY '\n';

-- Load the app data into temp table
LOAD DATA LOCAL INFILE '\Users\bmolaka\Desktop\Apple-DS\app_dat.csv'
INTO TABLE test.dummy_app_dat FIELDS TERMINATED BY ','
ENCLOSED BY '"' LINES TERMINATED BY '\n';

-- Load the in-app data into temp table
LOAD DATA LOCAL INFILE '\Users\bmolaka\Desktop\Apple-DS\in-app_dat.csv'
INTO TABLE test.dummy_inapp_dat FIELDS TERMINATED BY ','
ENCLOSED BY '"' LINES TERMINATED BY '\n';

-- Load the device data into temp table
LOAD DATA LOCAL INFILE '\Users\bmolaka\Desktop\Apple-DS\device_ref.csv'
INTO TABLE test.dummy_device_ref FIELDS TERMINATED BY ','
ENCLOSED BY '"' LINES TERMINATED BY '\n';

-- Load the app category data into temp table
LOAD DATA LOCAL INFILE '\Users\bmolaka\Desktop\Apple-DS\category_ref.csv'
INTO TABLE test.dummy_category_ref FIELDS TERMINATED BY ','
ENCLOSED BY '"' LINES TERMINATED BY '\n';


-- Clean the columns in dummy_transaction_dat by removing special characters
drop table if exists trans;
create temp table trans as 
(select 
cast(REPLACE(create_dt, '"', '') as date) as create_dt
,REPLACE(content_id, '"', '') as content_id
,REPLACE(acct_id, '"', '') as acct_id
,price
,device_id
from test.dummy_transaction_dat);

-- Found 2265 transactions to be duplicates. so remoing them to avoid redundancy.
drop table if exists trans_dedup;
create temp table trans_dedup as (
Select create_dt, content_id,acct_id,price,device_id
From 
( 
 Select create_dt, content_id,acct_id,price,device_id
 ,row_number() over (partition by create_dt, content_id,acct_id,device_id order by price) rno 
 From trans 
) 
Where rno = 1);

-- Combine all the data sets at transactions level (3,605,244 transactions)
drop table if exists final ;
create temp table final as (
SELECT 
 cast(a.create_dt as date) as trans_dt
,a.content_id
,a.acct_id
,a.price
,a.device_id
,cast(b.create_dt as date) as acc_create_dt
,b.payment_type
,coalesce(c.app_name,e.app_name) as app_name
,d.type as inapp_content_type
,f.device_name
,g.category_name
,datediff(day,b.create_dt,a.create_dt) as account_tenure_days
,datediff(month,b.create_dt,a.create_dt) as account_tenure_mons

from trans_dedup a
left join test.dummy_account_dat b
on a.acct_id=b.acct_id
left join test.dummy_app_dat c
on a.content_id=c.content_id
left join test.dummy_inapp_dat d
on a.content_id=d.content_id
left join test.dummy_app_dat e
on d.parent_app_content_id=e.content_id
left join test.dummy_device_ref f
on a.device_id=f.device_id
left join test.dummy_category_ref g
on c.category_id=g.category_id or e.category_id=g.category_id
);

-- create final transaction level dataset
drop table if exists transaction_final ;
create temp table transaction_final as (
select a.*
,b.acct_app_cnt
,date_part(dow, trans_dt) as trans_dow
,case when inapp_content_type is not null then 1 else 0 end as target
from final a
left join 
(select acct_id, count(distinct app_name) as acct_app_cnt from final group by 1) b
on a.acct_id=b.acct_id);

-- output the combined transaction data into a csv file
SELECT * FROM transaction_final
INTO OUTFILE '\Users\bmolaka\Desktop\Apple-DS\final_transaction_dat.csv'
FIELDS ENCLOSED BY '"'
TERMINATED BY ';'
ESCAPED BY '"'
LINES TERMINATED BY '\r\n';

-- create an app level dataset by left joining with other tables 
drop table if exists app_temp;
create temp table app_temp as 
(select 
 a.content_id
,a.app_name
,b.device_name
,c.category_name
,count(d.content_id) as app_download_cnt
,sum(d.price) as app_price
from test.dummy_app_dat a
left join test.dummy_device_ref b
on a.device_id=b.device_id
left join test.dummy_category_ref c
on a.category_id=c.category_id
left join trans_dedup d
on a.content_id=d.content_id
group by 1,2,3,4);

-- create final account dataset
drop table if exists app_temp1;
create temp table app_temp1 as 
(select 
 a.content_id
,b.type as content_type
,a.app_name
,a.device_name
,a.category_name
,a.app_download_cnt
,a.app_price
,count(c.content_id) as inapp_download_cnt
,sum(c.price) as inapp_price
from app_temp a
left join test.dummy_inapp_dat b
on a.content_id=b.parent_app_content_id
left join trans_dedup c
on b.content_id=c.content_id 
group by 1,2,3,4,5,6,7);

-- output the combined account data into a csv file
SELECT * FROM app_temp1
INTO OUTFILE '\Users\bmolaka\Desktop\Apple-DS\final_app_dat.csv'
FIELDS ENCLOSED BY '"'
TERMINATED BY ';'
ESCAPED BY '"'
LINES TERMINATED BY '\r\n';

-- create an account level dataset by left joining with other tables 
drop table if exists account_temp;
create temp table account_temp as 
(select 
 a.acct_id
,a.create_dt as acc_create_dt
,a.payment_type
,case when b.acct_id is not null then 1 else 0 end as trans_flg
,b.app_downloads as app_downloads
,b.app_price as app_price
,c.inapp_downloads as inapp_downloads
,c.inapp_price as inapp_price
,datediff(day,a.create_dt,'2016-09-22') as acct_tenure

from test.dummy_account_dat a
left join 
(select acct_id, count(content_id) as app_downloads, sum(price) as app_price 
    from trans_dedup where content_id in (select content_id from test.dummy_app_dat)
    group by 1) b
on a.acct_id=b.acct_id
left join (select acct_id, count(content_id) as inapp_downloads, sum(price) as inapp_price 
    from trans_dedup where content_id in (select content_id from test.dummy_inapp_dat)
    group by 1) c
on a.acct_id=c.acct_id
)

-- output the combined account level data into a csv file
SELECT * FROM account_temp
INTO OUTFILE '\Users\bmolaka\Desktop\Apple-DS\final_account_dat.csv'
FIELDS ENCLOSED BY '"'
TERMINATED BY ';'
ESCAPED BY '"'
LINES TERMINATED BY '\r\n';

/* Below are the three data sets that are output from this script for visualizations and further analysis.
\Users\bmolaka\Desktop\Apple-DS\final_transaction_dat.csv
\Users\bmolaka\Desktop\Apple-DS\final_app_dat.csv
\Users\bmolaka\Desktop\Apple-DS\final_account_dat.csv
*/


