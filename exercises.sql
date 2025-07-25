/* 02: Create first virtual warehouse */
use role SYSADMIN;

CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH WITH
  WAREHOUSE_SIZE = XSMALL
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;


/* Create a gen2 warehouse */
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH_GEN2 WITH
  RESOURCE_CONSTRAINT = STANDARD_GEN_2
  WAREHOUSE_SIZE = XSMALL
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

---------------------------------------------------------------------------------

/*
  Basic DB objects creation
*/

--create DB
CREATE DATABASE IF NOT EXISTS CITIBIKE;
use database citibike;

--create schema
CREATE schema IF NOT EXISTS WORK;


use role sysadmin;

--TRIPS table
create or replace table trips
(trip_id number,
 starttime timestamp_ntz,
 stoptime timestamp_ntz,
 duration number,
 start_station_id string,
 end_station_id string,
 trip_order string,
 bike_type string,
 bike_id string,
 user_name string,
 user_birth_date string,
 gender string,
 ride_type string,
 membership_type string,
 verification_method string
 );

---------------------------------------------------------------------------------

/* Authentication */

desc user sobottom;

--how to check from account perspective
select * from snowflake.account_usage.users where has_rsa_public_key;

--key generation
--private
--openssl genrsa 2048 | openssl pkcs8 -topk8 -v2 des3 -inform PEM -out rsa_key.p8

--public key:
--openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub

--assign public key to the user
 Alter user tomas set rsa_public_key = 'MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAsliFCYJ1WTRc1tU7XERe
WxYmJkn0FseOMwy3Vh8VbdZ3hXDAQGwqY9FdSXcSxJEqFYTM/D8dofmQLGPPPELX
QNYorbiZjD33HVagIhhfP72IdmYNXrzIvNRy+X7hadrMZpXRGmHqeM6UyevKHN9w
8OSWKKMXRGB6+TgzgGNty/izk4fVS+yFrCFnP0D6OAysJveODLfNt7Ux+16SfI+y
/iioXMWHeNGW/DOU3vbazIS86wev6YudiEg/ZGhD74iezsYlTFUW5DXeWhJ3NQ6w
EoSgrNli2ew93l0ynAYKwbmYyVdeHgN6nNjfC8/3JDlA/sywI4APTbb5YVKfjO7l
OQIDAQAB';

--can you guess how to do a key rotation?
desc user sobottom;

use role accountadmin;

--authentication policy to enforce MFA
CREATE OR REPLACE AUTHENTICATION POLICY mfa_enforcement_policy
  MFA_ENROLLMENT = 'REQUIRED'
  MFA_AUTHENTICATION_METHODS = ('PASSWORD');

--set to account
ALTER ACCOUNT SET AUTHENTICATION POLICY mfa_enforcement_policy;
--unset
ALTER ACCOUNT UNSET AUTHENTICATION POLICY;

--set to individual user
ALTER USER john SET AUTHENTICATION POLICY mfa_enforcement_policy;

/*encroll into MFA from user perspective
1. go to your profile
2. Settings
3. Authentication
4. Add a authentication method

*/

/*
RBAC model

*/

use role securityadmin;

create or replace role ANALYST;
grant usage on database citibike to role analyst;
grant usage on schema citibike.public to role analyst;
grant select on table citibike.public.trips to role analyst;
grant usage on warehouse compute_wh to role analyst;


create or replace role DEVELOPER;
grant role ANALYST to role DEVELOPER;
grant all on database CITIBIKE to role developer;
grant all on schema CITIBIKE.PUBLIC to role developer;
grant role developer to role SYSADMIN;
grant usage on warehouse compute_wh to role developer;


use role analyst;
create table tmp as select * from trips where 1 = 0;

use role developer;

create table tmp as select * from trips where 1 = 0;
drop table tmp;


/*
Data loading principles
Create file format
*/

CREATE OR REPLACE FILE FORMAT FF_CSV
TYPE = CSV
FIELD_DELIMITER = ','
SKIP_HEADER = 0
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
NULL_IF =''
error_on_column_count_mismatch=false;


desc file format ff_CSV;

/* Create a stage */
CREATE OR REPLACE STAGE S_CITIBIKE_TRIPS
URL = 's3://snowflake-workshop-lab/citibike-trips-csv/'
FILE_FORMAT = ( FORMAT_NAME = FF_CSV )
;

list @s_citibike_trips;

---------------------------------------------------------------------------------

/* COPY command */

--altering warehouse to be bigger
alter warehouse compute_wh set warehouse_size = large;


COPY into TRIPS from @S_CITIBIKE_TRIPS
ON_ERROR = SKIP_FILE; --376

alter warehouse compute_wh set warehouse_size = xsmall;


select count(*) from trips; limit 100; --1935271

select * from trips limit 100;



---------------------------------------------------------------------------------

/* WORKING WITH PARQUET DATA

First we have to offload the data into the stage. Let's use the user stage this time and
put the files under /parquet subdirectory. Please use COPY command option for header to include
column headers into the export: HEADER = true


Make a note how many records have been exported
*/

copy into @~/parquet/ FROM (
    select
        trip_id,
        starttime,
        stoptime,
        duration,
        start_station_id,
        end_station_id,
        trip_order,
        bike_type,
        bike_id,
        user_name,
        user_birth_date,
        gender,
        ride_type,
        membership_type,
        verification_method
    from trips
    where starttime >= '2024-02-01' and starttime < '2024-02-04'

)
FILE_FORMAT = (TYPE = PARQUET)
HEADER = true
OVERWRITE = true;



--remove @~/parquet/;

--list the files in user stage
ls @~/parquet;

/*CREATE copy of TRIPS table for loading from parquet */

create or replace table trips_parquet like trips;




/* Now let's upload the data from Parquet files placeced in user stage into the table TRIPS_PARQUET

When copying into the table do not forget to specify list of table columns similarly like you would
do in normal INSERT query.

use column names in uppercase as they are stored in uppercase in parquet files and referencing the elements in semi-structured files is case-sensitive.

COPY command will have following structure

COPY into trips_parquet (<<table columns>>)
FROM
(
    SELECT
        $1.column_name::column_data_type,
        ...
        ...
    FROM
       user stage/our subdirectory

)
FILE_FORMAT definition

*/

COPY into trips_parquet (trip_id, starttime, stoptime, duration, start_station_id,
        end_station_id, trip_order, bike_type, bike_id, user_name,
        user_birth_date, gender, ride_type, membership_type,
        verification_method)
from
(
 select
   $1:TRIP_ID::NUMBER(38, 0),
   $1:STARTTIME::TIMESTAMP_NTZ,
   $1:STOPTIME::TIMESTAMP_NTZ,
   $1:DURATION::NUMBER(38, 0),
   $1:START_STATION_ID::TEXT,
   $1:END_STATION_ID::TEXT,
   $1:TRIP_ORDER::TEXT,
   $1:BIKE_TYPE::TEXT,
   $1:BIKE_ID::TEXT,
   $1:USER_NAME::TEXT,
   $1:USER_BIRTH_DATE::TEXT,
   $1:GENDER::TEXT,
   $1:RIDE_TYPE::TEXT,
   $1:MEMBERSHIP_TYPE::TEXT,
   $1:VERIFICATION_METHOD::TEXT
 from @~/parquet/
)
 file_format = (type = PARQUET)
;

select * from trips_parquet limit 100; --1696547

--truncate trips_parquet;

create or replace file format my_parquet_format
type = parquet;

/* Using the infer_schema to find out the Parquet file structure

Now Let's suppose we are about to import a new parquet file where we do not know its structure.
Use INFER_SCHEMA function to find out how the file schema looks like.

Use the parquet files exported in previous steps. They are available in your user stage.

In order to use INFER_SCHEMA function, You need to have a file format with defined parquet type.
This one will be then used in INFER_SCHEMA calling

Please create following file format

create file format my_parquet_format
type = parquet;

*/

select *
  from table(
    infer_schema(
      location=>'@~/parquet/'
      , file_format=>'my_parquet_format'
      )
    );

--truncate table trips;

---------------------------------------------------------------------------------


/* Constructing JSONs */

--create a nested JSON from trips table
select * from trips limit 100;

select * from trips where date_trunc('day',starttime) between '2020-10-16' and '2020-10-18'
and start_station_id = 448;

--basic example for object_construct and array_agg
select
    object_construct(
     'StartStationId', start_station_id,
     'day',  date_trunc('day',starttime),
     'rideType', array_agg(distinct ride_type)  over (partition by date_trunc('day',starttime), start_station_id),
     'tripDetails', object_construct
        (
            'endStationId', end_station_id,
            'duration', duration
        )
   )

from trips
where
  date_trunc('day',starttime) between '2020-10-16' and '2020-10-18'
and start_station_id = 448;
limit 100;


select * from trips limit 100;

--complex example with CTE
with individual_trips as (
    select object_construct(
        'duration', duration,
        'endStation', end_station_id,
        'userbirthYear', user_birth_date,
        'membershipType', membership_type,
        'verificationMethod', verification_method
        ) t,
        start_station_id,
        starttime



from trips
where
 date_trunc('day',starttime) between '2020-10-16' and '2020-10-18'
and start_station_id = 448 )

select
    object_construct(
     'stationId', start_station_id,
      'day',  date_trunc('day',starttime),
      --'trips', t
      'trips', array_agg(t) over (partition by start_station_id, date_trunc('day',starttime) )
    ) json
from individual_trips limit 100;

---------------------------------------------------------------------------------

/*
Data Governance
Dynamic Data Masking
*/
use role sysadmin;
select * from trips limit 100;
grant select on table trips to role developer;

--policy creation


create or replace masking policy mask_pii as (val varchar) returns varchar ->
  case
    when current_role() in ('ANALYST', 'SYSADMIN', 'SECURITYADMIN', 'ACCOUNTADMIN') then val
    else '****'
end;

select * from table(information_schema.policy_references(policy_name => 'mask_pii'));

--applying masking policy to the birth_year and gender columns
alter table trips modify column user_birth_date set masking policy mask_pii;
alter table trips modify column gender set masking policy mask_pii;

--unsetting the policy
alter table trips modify column user_birth_date unset masking policy;
alter table trips modify column gender unset masking policy;

--changing role to DEVELOPER and checking the table again
use role developer;
select * from citibike.public.trips limit 100;

use role sysadmin;
select * from citibike.public.trips limit 100;

/* EXERCISE 2
same example but policy is automatically applied based on TAG value
unset the policy
*/
alter table trips modify column user_birth_date unset masking policy;
alter table trips modify column gender unset masking policy;

use role accountadmin;
drop tag security_level;
create or replace tag security_level;

alter tag security_level set masking policy mask_pii;

alter tag security_level unset masking policy mask_pii;

--assign tag to user_birth_date and gender columns
alter table trips modify column user_birth_date set tag security_level = 'pii';
alter table trips modify column gender set tag security_level = 'pii';

--remove assignment
alter table trips modify column user_birth_date unset tag security_level;
alter table trips modify column gender unset tag security_level;


--try to query a table
select * from citibike.public.trips limit 100;

--change role to developer and query the table again
use role developer;
select * from citibike.public.trips limit 100;

---------------------------------------------------------------------------------
/* Snowpipes */
use role accountadmin;

--Exercise 1 - Snowpipe demo

--create an storage integration
CREATE STORAGE INTEGRATION s3_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::749382034063:role/mySnowflakeRole'
  STORAGE_ALLOWED_LOCATIONS = ('s3://oreilly-trainings/');

--retrieve the AWS IAM User for my Snowflake account
desc integration s3_integration;
--STORAGE_AWS_IAM_USER_ARN: arn:aws:iam::407453656878:user/5ila-s-iest4676
--STORAGE_AWS_EXTERNAL_ID: BV72174_SFCRole=2_IM7FMEf0/yuiPOA9oAheHuuRXqk=

;show stages;

use role sysadmin;
--create a stage
create or replace stage snowpipe_stage
  url = 's3://oreilly-trainings/snowpipe/'
  storage_integration = s3_integration;


--create a target table
create or replace table snowpipe_landing (
id varchar,
first_name varchar,
last_name varchar,
email varchar,
gender varchar,
city varchar);

--create a snowpipe
create or replace pipe mypipe
    auto_ingest=true as
        copy into citibike.public.snowpipe_landing
        from @citibike.public.snowpipe_stage
        file_format = (type = 'CSV'
                      SKIP_HEADER = 1
                      FIELD_OPTIONALLY_ENCLOSED_BY='"');

--check the pipe to find out the notification channel for setting up the notifications
show pipes;
--notification-channel: arn:aws:sqs:eu-central-1:407453656878:sf-snowpipe-AIDAV5XRBM4XOLXNEJCZA-FIU6LnrizqM0eHFb-D9Hwg

use role accountadmin;

--pipe status
select system$pipe_status('mypipe');

--check the table
select * from snowpipe_landing;



truncate table snowpipe_landing;

--check the record count
select count(*) from snowpipe_landing;

---------------------------------------------------------------------------------

/* Multicluster warehouse */
/* Create a multicluster warehouse */
CREATE WAREHOUSE IF NOT EXISTS MULTI_COMPUTE_WH WITH
  WAREHOUSE_SIZE = SMALL
  MIN_CLUSTER_COUNT = 1
  MAX_CLUSTER_COUNT = 3
  SCALING_POLICY = ECONOMY
  AUTO_SUSPEND = 180
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

  ---------------------------------------------------------------------------------
/* Views */

select * from trips limit 100;

create or replace view trips_summary as
select
    date_trunc('month', starttime) ride_month,
    sum(duration) as total_duration,
    count(*) as number_of_trips,
    count(distinct user_name) unique_user_cnt
from trips
group by ride_month
order by ride_month;

select * from trips_summary;


--try to get a ddl for a view
select get_ddl('view', 'trips_summary');

--create a secure view
create or replace secure view trips_summary_secure as
select
    date_trunc('month', starttime) ride_month,
    sum(duration) as total_duration,
    count(*) as number_of_trips,
    count(distinct user_name) unique_user_cnt
from trips
group by ride_month
order by ride_month;
;

--query the secure view and check the query profile and notice the differences. There won't be visible what kind
--of operations have been performed by query optimizer
select * from tweets_summary_secure;

--try to get a ddl for secure_view and list of views as an owner
select get_ddl('view', 'trips_summary_secure');
show views;

--let's create add privileges for the secure view to our analyst role
use role securityadmin;
grant select on view citibike.public.trips_summary to role analyst;
grant select on view citibike.public.trips_summary_secure to role analyst;


use role analyst;
use secondary roles none;

--try to get DDL for secure view again
select get_ddl('view', 'trips_summary_secure');
--check the list of view - secure view will be missing the definition
show views;



select * from trips_summary_secure;


  ---------------------------------------------------------------------------------

  /* Resource monitors */
--resource monitors and their assignment to warehouses with monthly frequency (default value)
use role accountadmin;
CREATE OR REPLACE RESOURCE MONITOR rm_user_queries
  WITH CREDIT_QUOTA = 50
  FREQUENCY = MONTHLY
  START_TIMESTAMP = IMMEDIATELY
  TRIGGERS ON 75 PERCENT DO NOTIFY --notifications are sent to accountadmins
           ON 98 PERCENT DO SUSPEND
           ON 100 PERCENT DO SUSPEND_IMMEDIATE;

show resource monitors;

--assigning to warehouse
show warehouses;

alter warehouse compute_wh
set resource_monitor = rm_user_queries;

--account level monitor
CREATE OR REPLACE RESOURCE MONITOR rm_account WITH CREDIT_QUOTA=1000
  TRIGGERS ON 100 PERCENT DO SUSPEND;

ALTER ACCOUNT SET RESOURCE_MONITOR = rm_account;

--where to find them in UI: Admin -> Cost Management -> Resource Monitor

---------------------------------------------------------------------------------
/* user defined function */

use role sysadmin;
select * from snowpipe_landing limit 100;

--extract the domain from email UDF
create or replace function get_email_domain(email string)
returns string
language python
runtime_version = '3.9'
handler = 'domain_extractor'
as
$$
def domain_extractor(email):
    at_index = email.index('@')
    return email[at_index+1:]
$$;

--test the function
select email, get_email_domain(email)
from snowpipe_landing
where email is not null;

select * from snowpipe_landing where email is null ;

--extract the domain from email UDF
create or replace function get_email_domain(email string)
returns string
language python
runtime_version = '3.9'
handler = 'domain_extractor'
as
$$
def domain_extractor(email):
    if(email):
        at_index = email.index('@')
        return email[at_index+1:]
    else:
        return 'not valid email'
$$;

--test the function again for null values
select email, get_email_domain(email) from snowpipe_landing ;
--where email is null;


---------------------------------------------------------------------------------
/* Streams and tasks */

use role sysadmin;

--STREAMS
select date_trunc('month',starttime), count(*) from trips group by
date_trunc('month',starttime)
order by 1 desc;

--create table like trips and call it trips_monthly
create or replace table trips_monthly like trips;

--drop table trips_monthly;

--create a stream on top of this new table to track changes
create or replace stream str_trips_monthly on table trips_monthly;

--drop stream str_trips_monthly;

--check the stream;
show streams;

--check if stream has data
select SYSTEM$STREAM_HAS_DATA('str_trips_monthly');

--insert some data into table with stream
insert into trips_monthly select * from trips
where date_trunc('month', starttime) = '2024-01-01T00:00:00Z';

--check if stream has data again
select SYSTEM$STREAM_HAS_DATA('str_trips_monthly');

--try to query a stream
select * from str_trips_monthly limit 1000;

--check what kind of actions stream contains
select distinct metadata$action, metadata$isupdate from str_trips_monthly;


--create a table for holding the result of the aggregation
create or replace table fact_rides
(
month timestamp_ntz,
number_of_rides number,
total_duration number,
avg_duration number
);

--consume the stream and insert aggregated data into the new table
insert into fact_rides
select date_trunc('month', starttime), count(*), sum(duration), round(avg(duration),2)
from str_trips_monthly
group by date_trunc('month', starttime);

--check the data
select * from fact_rides;

--check if the stream still has data
select SYSTEM$STREAM_HAS_DATA('str_trips_monthly');

---------------------------------------------------------------------------------
/* EXERCISE 2
TASK
*/

--first we need to grant executing task to sysadmin role
use role accountadmin;
grant execute task on account to role sysadmin;
use role sysadmin;

--create task
create or replace task t_rides_agg
warehouse = COMPUTE_WH
schedule = '1 minute'
comment = 'aggregating rides on monhtly basis'
when SYSTEM$STREAM_HAS_DATA('str_trips_monthly')
AS
insert into fact_rides
select date_trunc('month', starttime), count(*), sum(duration), round(avg(duration),2)
from str_trips_monthly
group by date_trunc('month', starttime);

--check the task definition
show tasks;

--resume task
alter task t_rides_agg resume;

--insert data into trips_monthly
insert into trips_monthly select * from trips
where date_trunc('month', starttime) = '2024-02-01T00:00:00Z';

--check the fact table
select * from fact_rides;

--check the task history
select *
  from table(information_schema.task_history())
  order by scheduled_time desc;

---------------------------------------------------------------------------------

/* EXERCISE 3
Chaining the tasks to create a DAG
*/

--create a custom log table holding the last loaded month and system timestamp when it has been done
create or replace table log_fact_rides
(
max_loaded_month timestamp_ntz,
inserted_date timestamp_ntz
);

--we need to suspend the root task first
alter task t_rides_agg suspend;

--create a new task
create or replace task t_rides_log
warehouse = COMPUTE_WH
comment = 'Logging the last loaded month'
after T_RIDES_AGG
AS
insert into log_fact_rides select max(month), current_timestamp from
fact_rides;

--check the tasks
show tasks;

--resume both tasks
alter task t_rides_log resume;
alter task t_rides_agg resume;

alter task t_rides_agg suspend;

--insert new data into trips_monthly to trigger whole pipeline
insert into trips_monthly select * from trips
where date_trunc('month', starttime) = '2024-03-01T00:00:00Z';

--check fact table
select * from fact_rides;

--check the log table
select * from log_fact_rides;

------------------------------------------
--cleaning
truncate table fact_rides;
truncate table trips_monthly;

alter task t_rides_log suspend;
alter task t_rides_agg suspend;

------------------------------------------------------------------------------------
/* Semi structured data flattening */

--first we need to create some JSON structure by using functions ARRAY_AGG and OBJECT_CONSTRUCT
with individual_trips as (
    select object_construct(
        'startStation', start_station_id,
        'duration', duration,
        'endStation', end_station_id,
        'userbirthYear', user_birth_date,
        'membershipType', membership_type,
        'verificationMethod', verification_method
        ) t,
        start_station_id,
        starttime



from trips
where
date_trunc('day',starttime) between '2020-10-16' and '2020-10-18'
and start_station_id = 448
 )

select
    object_construct(
     'stationStation', start_station_id,
      'day',  date_trunc('day',starttime),
      --'trips', t
      'trips', array_agg(t) over (partition by start_station_id, date_trunc('day',starttime) )
    ) json
from individual_trips limit 100;



--then let's create a table with those json data
create or replace table json_trips_per_station as
with individual_trips as (
    select object_construct(
        'startStation', start_station_id,
        'duration', duration,
        'endStation', end_station_id,
        'membershipType', membership_type,
        'userDetails', object_construct(
            'userName', user_name,
            'userbirthYear', user_birth_date
        )

        ) t,
        start_station_id,
        starttime



from trips
where
date_trunc('day',starttime) between '2020-10-16' and '2020-10-18'
and start_station_id = 448
 )

select
    object_construct(
     'stationName', start_station_id,
      'day',  date_trunc('day',starttime),
      --'trips', t
      'trips', array_agg(t) over (partition by start_station_id, date_trunc('day',starttime) )
    ) json
from individual_trips;


select count(*) from json_trips_per_station;
--we can query that table now
select * from json_trips_per_station limit 100;

--and finally flattening the data
select
t.json:day::timestamp start_time,
t.json:stationName::varchar start_station,
f.value:duration::number duration,
f.value:endStation::varchar end_station,
f.value:membershipType::varchar membership_Type,
f.value:userDetails:userName::varchar user_name,
f.value:userDetails:userbirthYear::varchar user_Birth_Year
from
json_trips_per_station t,
lateral flatten (input => t.json:trips) f
limit 10;


------------------------------------------------------------------------------------
