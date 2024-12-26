use role sysadmin;
use database sandbox;
use warehouse adho_wh;
use schema stage_sch;



create or replace table stage_sch.restaurant (
    restaurantid text,
    name text,
    cuisinetype text,
    pricing_for_2 text,                                  
    restaurant_phone text WITH TAG (common.pii_policy_tag = 'SENSITIVE'),                               -- phone number as text
    operatinghours text,                                 
    locationid text ,                                    
    activeflag text ,                                    -- active status
    openstatus text ,                                    -- open status
    locality text,                                       -- locality as text
    restaurant_address text,                             -- address as text
    latitude text,                                       -- latitude as text for precision
    longitude text,                                      -- longitude as text for precision
    createddate text,                                    -- record creation date
    modifieddate text,                                   -- last modified date

    -- audit columns for debugging
    _stg_file_name text,
    _stg_file_load_ts timestamp,
    _stg_file_md5 text,
    _copy_data_ts timestamp default current_timestamp
)
comment = 'This is the restaurant stage/raw table where data will be copied from internal stage using copy command. This is as-is data represetation from the source location. All the columns are text data type except the audit columns that are added for traceability.'
;


create or replace stream stage_sch.restaurant_stm 
on table stage_sch.restaurant 
append_only = true
;


copy into stage_sch.restaurant (restaurantid, name, cuisinetype, pricing_for_2, restaurant_phone, 
                      operatinghours, locationid, activeflag, openstatus, 
                      locality, restaurant_address, latitude, longitude, 
                      createddate, modifieddate, 
                      _stg_file_name, _stg_file_load_ts, _stg_file_md5, _copy_data_ts)
from (
    select 
        t.$1::text as restaurantid,        -- restaurantid as the first column
        t.$2::text as name,
        t.$3::text as cuisinetype,
        t.$4::text as pricing_for_2,
        t.$5::text as restaurant_phone,
        t.$6::text as operatinghours,
        t.$7::text as locationid,
        t.$8::text as activeflag,
        t.$9::text as openstatus,
        t.$10::text as locality,
        t.$11::text as restaurant_address,
        t.$12::text as latitude,
        t.$13::text as longitude,
        t.$14::text as createddate,
        t.$15::text as modifieddate,
        -- audit columns for tracking & debugging
        metadata$filename as _stg_file_name,
        metadata$file_last_modified as _stg_file_load_ts,
        metadata$file_content_key as _stg_file_md5,
        current_timestamp() as _copy_data_ts
    from @stage_sch.csv_stage/initial/restaurant/restaurant-delhi+NCR.csv t
)
file_format = (format_name = 'stage_sch.csv_file_format')
on_error = abort_statement;

select * from stage_sch.restaurant;
select * from stage_sch.restaurant_stm;

----------------------------------------------

use schema clean_sch;

create or replace table clean_sch.restaurant ( 
    restaurant_sk number autoincrement primary key,
    restaurant_id number unique,
    name string(100) not null,
    cuisine_type string,
    pricing_for_two number(10,2),
    restaurant_phone string(15) with tag (common.pii_policy_tag ='SENSITIVE'),
    operating_hours string(100),
    location_id_fk number,
    active_flag string(100),
    open_status string(100),
    locality string(100),
    restaurant_address string,
    latitude number(9,6),
    longitude number(9,6),
    created_dt timestamp_tz,
    modified_dt timestamp_tz,

    ------additional audit columns
    _stg_file_name string,
    _stg_file_load_ts timestamp_ntz,
    _stg_file_md5 string,
    _copy_data_ts timestamp_ntz default current_timestamp
    );

create or replace stream clean_sch.restaurant_stm 
on table clean_sch.restaurant ;

select * from clean_sch.restaurant ;


------Merge statement 
merge into clean_sch.restaurant as target 
using (
    select 
        try_cast(restaurantid AS number) AS restaurant_id,
        try_cast(name AS string) AS name,
        try_cast(cuisinetype AS string) AS cuisine_type,
        try_cast(pricing_for_2 AS number(10, 2)) AS pricing_for_two,
        try_cast(restaurant_phone AS string) AS restaurant_phone,
        try_cast(operatinghours AS string) AS operating_hours,
        try_cast(locationid AS number) AS location_id_fk,
        try_cast(activeflag AS string) AS active_flag,
        try_cast(openstatus AS string) AS open_status,
        try_cast(locality AS string) AS locality,
        try_cast(restaurant_address AS string) AS restaurant_address,
        try_cast(latitude AS number(9, 6)) AS latitude,
        try_cast(longitude AS number(9, 6)) AS longitude,
        try_to_timestamp_ntz(createddate, 'YYYY-MM-DD HH24:MI:SS.FF9') AS created_dt,
        try_to_timestamp_ntz(modifieddate, 'YYYY-MM-DD HH24:MI:SS.FF9') AS modified_dt,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5
    FROM 
        stage_sch.restaurant_stm
) as source 
on target.restaurant_id = source.restaurant_id

when matched then 
    update set 
        target.name = source.name,
        target.cuisine_type = source.cuisine_type,
        target.pricing_for_two = source.pricing_for_two,
        target.restaurant_phone = source.restaurant_phone,
        target.operating_hours = source.operating_hours,
        target.location_id_fk = source.location_id_fk,
        target.active_flag = source.active_flag,
        target.open_status = source.open_status,
        target.locality = source.locality,
        target.restaurant_address = source.restaurant_address,
        target.latitude = source.latitude,
        target.longitude = source.longitude,
        target.created_dt = source.created_dt,
        target.modified_dt = source.modified_dt,
        target._stg_file_name = source._stg_file_name,
        target._stg_file_load_ts = source._stg_file_load_ts,
        target._stg_file_md5 = source._stg_file_md5
when not matched then
    insert( 
        restaurant_id,
        name,
        cuisine_type,
        pricing_for_two,
        restaurant_phone,
        operating_hours,
        location_id_fk,
        active_flag,
        open_status,
        locality,
        restaurant_address,
        latitude,
        longitude,
        created_dt,
        modified_dt,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5
        )
    values (
        source.restaurant_id,
        source.name,
        source.cuisine_type,
        source.pricing_for_two,
        source.restaurant_phone,
        source.operating_hours,
        source.location_id_fk,
        source.active_flag,
        source.open_status,
        source.locality,
        source.restaurant_address,
        source.latitude,
        source.longitude,
        source.created_dt,
        source.modified_dt,
        source._stg_file_name,
        source._stg_file_load_ts,
        source._stg_file_md5
    );

    select * from clean_sch.restaurant_stm;



create or replace table consumption_sch.restaurant_dim (
    restaurant_hk number primary key,
    restaurant_id number unique,
    name string(100) not null,
    cuisine_type string,
    pricing_for_two number(10,2),
    restaurant_phone string(15) with tag (common.pii_policy_tag ='SENSITIVE'),
    operating_hours string(100),
    location_id_fk number,
    active_flag string(100),
    open_status string(100),
    locality string(100),
    restaurant_address string,
    latitude number(9,6),
    longitude number(9,6),
    eff_start_date timestamp_tz,  -- Effective start date for the record
    eff_end_date timestamp_tz,    -- Effective end date for the record (NULL if active)
    is_current boolean            -- Indicates whether the record is the current version
);


select * from clean_sch.restaurant_stm;


----merge into the restaurant dim table

merge into consumption_sch.restaurant_dim as target 
using clean_sch.restaurant_stm as source 
on 
    target.RESTAURANT_ID = source.RESTAURANT_ID AND 
    target.NAME = source.NAME AND 
    target.CUISINE_TYPE = source.CUISINE_TYPE AND 
    target.PRICING_FOR_TWO = source.PRICING_FOR_TWO AND 
    target.RESTAURANT_PHONE = source.RESTAURANT_PHONE AND 
    target.OPERATING_HOURS = source.OPERATING_HOURS AND 
    target.LOCATION_ID_FK = source.LOCATION_ID_FK AND 
    target.ACTIVE_FLAG = source.ACTIVE_FLAG AND 
    target.OPEN_STATUS = source.OPEN_STATUS AND 
    target.LOCALITY = source.LOCALITY AND 
    target.RESTAURANT_ADDRESS = source.RESTAURANT_ADDRESS AND 
    target.LATITUDE = source.LATITUDE AND 
    target.LONGITUDE = source.LONGITUDE

    when matched 
    and source.METADATA$ACTION = 'DELETE' AND source.METADATA$ISUPDATE = 'TRUE' THEN
    -- Update the existing record to close its validity period
        UPDATE SET 
            target.EFF_END_DATE = CURRENT_TIMESTAMP(),
            target.IS_CURRENT = FALSE

    when not matched     
    and source.METADATA$ACTION = 'INSERT' AND source.METADATA$ISUPDATE = 'FALSE' THEN
        insert (
            RESTAURANT_HK,
        RESTAURANT_ID,
        NAME,
        CUISINE_TYPE,
        PRICING_FOR_TWO,
        RESTAURANT_PHONE,
        OPERATING_HOURS,
        LOCATION_ID_FK,
        ACTIVE_FLAG,
        OPEN_STATUS,
        LOCALITY,
        RESTAURANT_ADDRESS,
        LATITUDE,
        LONGITUDE,
        EFF_START_DATE,
        EFF_END_DATE,
        IS_CURRENT
        )
    values (
        hash(SHA1_hex(CONCAT(source.RESTAURANT_ID, source.NAME, source.CUISINE_TYPE, 
            source.PRICING_FOR_TWO, source.RESTAURANT_PHONE, source.OPERATING_HOURS, 
            source.LOCATION_ID_FK, source.ACTIVE_FLAG, source.OPEN_STATUS, source.LOCALITY, 
            source.RESTAURANT_ADDRESS, source.LATITUDE, source.LONGITUDE))),
        source.RESTAURANT_ID,
        source.NAME,
        source.CUISINE_TYPE,
        source.PRICING_FOR_TWO,
        source.RESTAURANT_PHONE,
        source.OPERATING_HOURS,
        source.LOCATION_ID_FK,
        source.ACTIVE_FLAG,
        source.OPEN_STATUS,
        source.LOCALITY,
        source.RESTAURANT_ADDRESS,
        source.LATITUDE,
        source.LONGITUDE,
        CURRENT_TIMESTAMP(),
        NULL,
        TRUE
    );
        
    select * from consumption_sch.restaurant_dim;


--load the delta data

copy into stage_sch.restaurant (restaurantid, name, cuisinetype, pricing_for_2, restaurant_phone, 
                      operatinghours, locationid, activeflag, openstatus, 
                      locality, restaurant_address, latitude, longitude, 
                      createddate, modifieddate, 
                      _stg_file_name, _stg_file_load_ts, _stg_file_md5, _copy_data_ts)
from (
    select 
    t.$1::text as restaurantid,        -- restaurantid as the first column
        t.$2::text as name,
        t.$3::text as cuisinetype,
        t.$4::text as pricing_for_2,
        t.$5::text as restaurant_phone,
        t.$6::text as operatinghours,
        t.$7::text as locationid,
        t.$8::text as activeflag,
        t.$9::text as openstatus,
        t.$10::text as locality,
        t.$11::text as restaurant_address,
        t.$12::text as latitude,
        t.$13::text as longitude,
        t.$14::text as createddate,
        t.$15::text as modifieddate,
        metadata$filename as _stg_file_name,
        metadata$file_last_modified as _stg_file_load_ts,
        metadata$file_content_key as _stg_file_md5,
        current_timestamp() as _copy_data_ts
     from @stage_sch.csv_stage/delta/restaurant/day-01-insert-restaurant-delhi+NCR.csv t

)
file_format= (format_name= 'stage_sch.csv_file_format')
on_error = abort_statement;


select * from stage_sch.restaurant;
select * from stage_sch.restaurant_stm;
     
    
copy into stage_sch.restaurant (restaurantid, name, cuisinetype, pricing_for_2, restaurant_phone, 
                      operatinghours, locationid, activeflag, openstatus, 
                      locality, restaurant_address, latitude, longitude, 
                      createddate, modifieddate, 
                      _stg_file_name, _stg_file_load_ts, _stg_file_md5, _copy_data_ts)
from (
    select 
    t.$1::text as restaurantid,        -- restaurantid as the first column
        t.$2::text as name,
        t.$3::text as cuisinetype,
        t.$4::text as pricing_for_2,
        t.$5::text as restaurant_phone,
        t.$6::text as operatinghours,
        t.$7::text as locationid,
        t.$8::text as activeflag,
        t.$9::text as openstatus,
        t.$10::text as locality,
        t.$11::text as restaurant_address,
        t.$12::text as latitude,
        t.$13::text as longitude,
        t.$14::text as createddate,
        t.$15::text as modifieddate,
        metadata$filename as _stg_file_name,
        metadata$file_last_modified as _stg_file_load_ts,
        metadata$file_content_key as _stg_file_md5,
        current_timestamp() as _copy_data_ts
     from @stage_sch.csv_stage/delta/restaurant/day-02-upsert-restaurant-delhi+NCR.csv t

)
file_format= (format_name= 'stage_sch.csv_file_format')
on_error = abort_statement;


select * from stage_sch.restaurant;
select * from stage_sch.restaurant_stm;

