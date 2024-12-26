use role sysadmin;
use database sandbox;
use warehouse adho_wh;
use schema stage_sch;


create or replace table stage_sch.customer (
    customerid text,
    name text,
    mobile text with tag (common.pii_policy_tag = 'PII'),
    email text with tag (common.pii_policy_tag ='EMAIL'),
    loginbyusing text,
    gender text with tag (common.pii_policy_tag = 'PII'),
    dob text with tag (common.pii_policy_tag ='PII'),
    anniversary text,
    preferences text,
    createddate text, 
    modifieddate text,

    ---audit columns with appropiate data types
    _stg_file_name text,
    _stg_file_load_ts timestamp,
    _stg_file_md5 text,
    _copy_data_ts timestamp default current_timestamp
    
);

--create a stream on this table
create or replace stream stage_sch.customer_stm 
on table stage_sch.customer 
append_only = true
;


--copy initial data into the customer table

copy into stage_sch.customer (customerid, name, mobile, email, loginbyusing, gender, dob, anniversary, 
                    preferences, createddate, modifieddate, 
                    _stg_file_name, _stg_file_load_ts, _stg_file_md5, _copy_data_ts)
from (
    select 
    t.$1::text as customerid,
    t.$2::text as name,
    t.$3::text as mobile,
    t.$4::text as email,
    t.$5::text as loginbyusing,
    t.$6::text as gender,
    t.$7::text as dob,
    t.$8::text as anniversary,
    t.$9::text as preferences,
    t.$10::text as createddate,
    t.$11::text as modifieddate,
    metadata$filename as _stg_file_name,
    metadata$file_last_modified as _stg_file_load_ts,
    metadata$file_content_key as _stg_file_md5,
    current_timestamp as _copy_data_ts
    from @stage_sch.csv_stage/initial/customer/customers-initial.csv t
)
file_format = (format_name= 'stage_sch.csv_file_format')
on_error = abort_statement;


select * from stage_sch.customer_stm ;
select * from stage_sch.customer;

create or replace table clean_sch.customer (
    customer_sk number autoincrement primary key,
    customer_id string not null,
    name string(100) not null,
    mobile string(15) with tag (common.pii_policy_tag ='PII'),
    email string(100) with tag (common.pii_policy_tag ='EMAIL'),
    login_by_using string(50),
    gender string(10) with tag (common.pii_policy_tag ='PII'),
    dob date with tag (common.pii_policy_tag='PII'),
    anniversary date,
    preferences string,
    created_dt timestamp_tz,
    modified_dt timestamp_tz,
    _stg_file_name string,
    _stg_file_load_ts timestamp_ntz,
    _stg_file_md5 string,
    _copy_data_ts timestamp_ntz default current_timestamp
    );


--create a stream on this table
create or replace stream clean_sch.customer_stm
on table clean_sch.customer 
;

--merge into the table 

merge into clean_sch.customer as target 
using(
    select 
        customerid::string as customer_id,
        name::string as name,
        mobile::string as mobile,
        email::string as email,
        loginbyusing::string as login_by_using,
        gender::string as gender,
        try_to_date(dob, 'YYYY-MM-DD') as dob,
        try_to_date(anniversary, 'YYYY-MM-DD') as anniversary,
        preferences::string as preferences , 
        try_to_timestamp(createddate, 'YYYY-MM-DD"T"HH24:MI:SS.FF6') as created_dt,
        try_to_timestamp(modifieddate, 'YYYY-MM-DD"T"HH24:MI:SS.FF6') as modified_dt,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        _copy_data_ts
    from stage_sch.customer_stm 
) as source

on target.customer_id = source.customer_id

when matched then
    update set 
        target.NAME = source.NAME,
        target.MOBILE = source.MOBILE,
        target.EMAIL = source.EMAIL,
        target.LOGIN_BY_USING = source.LOGIN_BY_USING,
        target.GENDER = source.GENDER,
        target.DOB = source.DOB,
        target.ANNIVERSARY = source.ANNIVERSARY,
        target.PREFERENCES = source.PREFERENCES,
        target.CREATED_DT = source.CREATED_DT,
        target.MODIFIED_DT = source.MODIFIED_DT,
        target._STG_FILE_NAME = source._STG_FILE_NAME,
        target._STG_FILE_LOAD_TS = source._STG_FILE_LOAD_TS,
        target._STG_FILE_MD5 = source._STG_FILE_MD5,
        target._COPY_DATA_TS = source._COPY_DATA_TS

when not matched then
    insert (
        CUSTOMER_ID,
        NAME,
        MOBILE,
        EMAIL,
        LOGIN_BY_USING,
        GENDER,
        DOB,
        ANNIVERSARY,
        PREFERENCES,
        CREATED_DT,
        MODIFIED_DT,
        _STG_FILE_NAME,
        _STG_FILE_LOAD_TS,
        _STG_FILE_MD5,
        _COPY_DATA_TS
    )
    VALUES (
        source.CUSTOMER_ID,
        source.NAME,
        source.MOBILE,
        source.EMAIL,
        source.LOGIN_BY_USING,
        source.GENDER,
        source.DOB,
        source.ANNIVERSARY,
        source.PREFERENCES,
        source.CREATED_DT,
        source.MODIFIED_DT,
        source._STG_FILE_NAME,
        source._STG_FILE_LOAD_TS,
        source._STG_FILE_MD5,
        source._COPY_DATA_TS
    );
    select * from clean_sch.customer_stm;

--create the dimension table

create or replace table consumption_sch.customer_dim 
(
    customer_hk number primary key,
    customer_id string not null,
    name string,
    mobile string(15) with tag (common.pii_policy_tag = 'PII'),
    email string(100) with tag (common.pii_policy_tag='EMAIL'),
    login_by_using string(50),
    gender string(10) with tag (common.pii_policy_tag='PII'),
    dob date with tag (common.pii_policy_tag='PII'),
    anniversary date,
    preferences string,
    --------- scd2 handling
    eff_start_date timestamp_tz,
    eff_end_date timestamp_tz,
    is_current boolean
);


--merge into the dimension table 

merge into 
    consumption_sch.customer_dim as target
using 
    clean_sch.customer_stm as source 
on  
    target.CUSTOMER_ID = source.CUSTOMER_ID AND
    target.NAME = source.NAME AND
    target.MOBILE = source.MOBILE AND
    target.EMAIL = source.EMAIL AND
    target.LOGIN_BY_USING = source.LOGIN_BY_USING AND
    target.GENDER = source.GENDER AND
    target.DOB = source.DOB AND
    target.ANNIVERSARY = source.ANNIVERSARY AND
    target.PREFERENCES = source.PREFERENCES
WHEN MATCHED 
    AND source.METADATA$ACTION = 'DELETE' AND source.METADATA$ISUPDATE = 'TRUE' THEN
    -- Update the existing record to close its validity period
    UPDATE SET 
        target.EFF_END_DATE = CURRENT_TIMESTAMP(),
        target.IS_CURRENT = FALSE
WHEN NOT MATCHED 
    AND source.METADATA$ACTION = 'INSERT' AND source.METADATA$ISUPDATE = 'FALSE' THEN
    -- Insert new record with current data and new effective start date
    INSERT (
        CUSTOMER_HK,
        CUSTOMER_ID,
        NAME,
        MOBILE,
        EMAIL,
        LOGIN_BY_USING,
        GENDER,
        DOB,
        ANNIVERSARY,
        PREFERENCES,
        EFF_START_DATE,
        EFF_END_DATE,
        IS_CURRENT
    )
    VALUES (
        hash(SHA1_hex(CONCAT(source.CUSTOMER_ID, source.NAME, source.MOBILE, 
            source.EMAIL, source.LOGIN_BY_USING, source.GENDER, source.DOB, 
            source.ANNIVERSARY, source.PREFERENCES))),
        source.CUSTOMER_ID,
        source.NAME,
        source.MOBILE,
        source.EMAIL,
        source.LOGIN_BY_USING,
        source.GENDER,
        source.DOB,
        source.ANNIVERSARY,
        source.PREFERENCES,
        CURRENT_TIMESTAMP(),
        NULL,
        TRUE
    );

copy into stage_sch.customer (customerid, name, mobile, email, loginbyusing, gender, dob, anniversary, 
                    preferences, createddate, modifieddate, 
                    _stg_file_name, _stg_file_load_ts, _stg_file_md5, _copy_data_ts)
from ( 
    select 
        t.$1::text as customerid,
        t.$2::text as name,
        t.$3::text as mobile,
        t.$4::text as email,
        t.$5::text as loginbyusing,
        t.$6::text as gender,
        t.$7::text as dob,
        t.$8::text as anniversary,
        t.$9::text as preferences,
        t.$10::text as createddate,
        t.$11::text as modifieddate,
        metadata$filename as _stg_file_name,
        metadata$file_last_modified as _stg_file_load_ts,
        metadata$file_content_key as _stg_file_md5,
        current_timestamp as _copy_data_ts
    from @stage_sch.csv_stage/delta/customer/day-01-insert-customer.csv t
)
file_format= (format_name = 'stage_sch.csv_file_format')
on_error = abort_statement ;

copy into stage_sch.customer (customerid, name, mobile, email, loginbyusing, gender, dob, anniversary, 
                    preferences, createddate, modifieddate, 
                    _stg_file_name, _stg_file_load_ts, _stg_file_md5, _copy_data_ts)
from ( 
    select 
        t.$1::text as customerid,
        t.$2::text as name,
        t.$3::text as mobile,
        t.$4::text as email,
        t.$5::text as loginbyusing,
        t.$6::text as gender,
        t.$7::text as dob,
        t.$8::text as anniversary,
        t.$9::text as preferences,
        t.$10::text as createddate,
        t.$11::text as modifieddate,
        metadata$filename as _stg_file_name,
        metadata$file_last_modified as _stg_file_load_ts,
        metadata$file_content_key as _stg_file_md5,
        current_timestamp as _copy_data_ts
    from @stage_sch.csv_stage/delta/customer/day-02-insert-update.csv t
)
file_format= (format_name = 'stage_sch.csv_file_format')
on_error = abort_statement ;


select * from stage_sch.customer;
select * from stage_sch.customer_stm;
    
select * from consumption_sch.customer_dim ;
