use role sysadmin;
use warehouse adho_wh;
use database sandbox;
use schema stage_sch;



create or replace table stage_sch.location (
    locationid text,
    city text,
    state text,
    zipcode text,
    activeflag text,
    createdate text , 
    modifieddate text,
    --audit columns for tracking & debugging
    _stg_file_name text,
    _stg_file_load_ts timestamp,
    _stg_file_md5 text,
    _copy_data_ts timestamp default current_timestamp
)

comment = 'this table is where raw data will be copied from internal stage using copy into command'
;

-- create a stream object to track data and manage new added ones

create or replace stream stage_sch.location_stm 
on table stage_sch.location 
append_only = true
comment ='this stream capture only inserts on the location table' ;

select * from location;

--now let's copy data into or table

copy into stage_sch.location (locationid,city,state,zipcode,activeflag,createdate,modifieddate,
                            _stg_file_name,_stg_file_load_ts,_stg_file_md5,_copy_data_ts)
from (
    select
        t.$1 ::text as locationid,
        t.$2 ::text as city,
        t.$3 ::text as state,
        t.$4 ::text as zipcode,
        t.$5 ::text as activeflag,
        t.$6 ::text as createddate,
        t.$7 ::text as modifieddate,
        metadata$filename as _stg_file_name,
        metadata$file_last_modified as _stg_file_load_ts,
        metadata$file_content_key as _stg_file_md5,
        current_timestamp as _copy_data_ts
        
    from @stage_sch.csv_stage/initial/location t
)
file_format =(format_name= 'stage_sch.csv_file_format')
on_error = abort_statement;


select * from location;
select * from stage_sch.location_stm ;

---------------------------------------------------------------------

use schema clean_sch;
create or replace table clean_sch.restaurant_location (
    restaurant_location_sk number autoincrement primary key,
    location_id number not null unique,
    city string(100) not null,
    state string (100) not null,
    state_code string(2) not null,
    is_union_territory boolean not null default false,
    capital_city_flag boolean not null default false,
    city_tier text(6),
    zip_code string(10) not null,
    active_flag string(10) not null,
    created_ts timestamp_tz not null,
    modified_ts timestamp_tz,

    --additional audit columns 
    _stg_file_name string,
    _stg_file_load_ts timestamp_ntz,
    _stg_file_md5 string,
    _copy_data_ts timestamp_ntz default current_timestamp
    
)

comment = 'data with appropriate data type under clean schema, data is populated using merge statement from the stage layer location table' ;


create or replace stream clean_sch.restaurant_location_stm
on table clean_sch.restaurant_location

comment =' this stream capture any changes made on restaurant location table'
;


merge into clean_sch.restaurant_location as target
using (
    select
        cast(LocationId as number) as location_ID,
        cast(city as string) as city,
        case 
            when cast(state as string) = 'Delhi' then 'New Delhi'
            else cast(state as string)
        end as state,
        ---- state code
        CASE 
            WHEN State = 'Delhi' THEN 'DL'
            WHEN State = 'Maharashtra' THEN 'MH'
            WHEN State = 'Uttar Pradesh' THEN 'UP'
            WHEN State = 'Gujarat' THEN 'GJ'
            WHEN State = 'Rajasthan' THEN 'RJ'
            WHEN State = 'Kerala' THEN 'KL'
            WHEN State = 'Punjab' THEN 'PB'
            WHEN State = 'Karnataka' THEN 'KA'
            WHEN State = 'Madhya Pradesh' THEN 'MP'
            WHEN State = 'Odisha' THEN 'OR'
            WHEN State = 'Chandigarh' THEN 'CH'
            WHEN State = 'West Bengal' THEN 'WB'
            WHEN State = 'Sikkim' THEN 'SK'
            WHEN State = 'Andhra Pradesh' THEN 'AP'
            WHEN State = 'Assam' THEN 'AS'
            WHEN State = 'Jammu and Kashmir' THEN 'JK'
            WHEN State = 'Puducherry' THEN 'PY'
            WHEN State = 'Uttarakhand' THEN 'UK'
            WHEN State = 'Himachal Pradesh' THEN 'HP'
            WHEN State = 'Tamil Nadu' THEN 'TN'
            WHEN State = 'Goa' THEN 'GA'
            WHEN State = 'Telangana' THEN 'TG'
            WHEN State = 'Chhattisgarh' THEN 'CG'
            WHEN State = 'Jharkhand' THEN 'JH'
            WHEN State = 'Bihar' THEN 'BR'
            ELSE NULL
        END AS state_code,

        case 
            when state in ('Delhi', 'Chandigarh', 'Puducherry', 'Jammu and Kashmir') then 'Y'
            else 'N'

        end as is_union_territory,

        case 
            when (state = 'Delhi' and city = 'New Delhi') then true
            when (state ='Maharashtra' and city ='Mumbai') then true

            else false
        end as capital_city_flag,

        case 
            when city in ('Mumbai', 'Delhi', 'Bengaluru', 'Hyderabad', 'Chennai', 'Kolkata', 'Pune', 'Ahmedabad') THEN 'Tier-1'
            WHEN City IN ('Jaipur', 'Lucknow', 'Kanpur', 'Nagpur', 'Indore', 'Bhopal', 'Patna', 'Vadodara', 'Coimbatore', 
                          'Ludhiana', 'Agra', 'Nashik', 'Ranchi', 'Meerut', 'Raipur', 'Guwahati', 'Chandigarh') THEN 'Tier-2'
            ELSE 'Tier-3'
        END AS city_tier,

        cast(zipcode as string) as zip_code,
        cast(activeflag as string) as active_flag,
        to_timestamp_tz(createdate, 'YYYY-MM-DD HH24:MI:SS') as created_ts,
        to_timestamp_tz(modifieddate, 'YYYY-MM-DD HH24:MI:SS') as modified_ts,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        CURRENT_TIMESTAMP AS _copy_data_ts
    FROM stage_sch.location_stm
        
) as source

on target.location_id = source.location_id 
when matched and (
    target.City != source.City OR
    target.State != source.State OR
    target.state_code != source.state_code OR
    target.is_union_territory != source.is_union_territory OR
    target.capital_city_flag != source.capital_city_flag OR
    target.city_tier != source.city_tier OR
    target.Zip_Code != source.Zip_Code OR
    target.Active_Flag != source.Active_Flag OR
    target.modified_ts != source.modified_ts
) THEN 
    UPDATE SET 
        target.City = source.City,
        target.State = source.State,
        target.state_code = source.state_code,
        target.is_union_territory = source.is_union_territory,
        target.capital_city_flag = source.capital_city_flag,
        target.city_tier = source.city_tier,
        target.Zip_Code = source.Zip_Code,
        target.Active_Flag = source.Active_Flag,
        target.modified_ts = source.modified_ts,
        target._stg_file_name = source._stg_file_name,
        target._stg_file_load_ts = source._stg_file_load_ts,
        target._stg_file_md5 = source._stg_file_md5,
        target._copy_data_ts = source._copy_data_ts
    
when not matched then 
    insert (
        Location_ID,
        City,
        State,
        state_code,
        is_union_territory,
        capital_city_flag,
        city_tier,
        Zip_Code,
        Active_Flag,
        created_ts,
        modified_ts,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        _copy_data_ts
    )
    VALUES (
        source.Location_ID,
        source.City,
        source.State,
        source.state_code,
        source.is_union_territory,
        source.capital_city_flag,
        source.city_tier,
        source.Zip_Code,
        source.Active_Flag,
        source.created_ts,
        source.modified_ts,
        source._stg_file_name,
        source._stg_file_load_ts,
        source._stg_file_md5,
        source._copy_data_ts
    );
select * from clean_sch.restaurant_location;
select * from  clean_sch.restaurant_location_stm;


create or replace table consumption_sch.restaurant_location_dim (
    restaurant_location_hk number primary key ,
    location_id number(38,0) not null,
    city varchar(100) not null,
    state varchar(100) not null,
    state_code varchar(2) not null,
    is_union_territory boolean not null default false,
    capital_city_flag boolean not null default false,
    city_tier varchar(6),                              
    zip_code varchar(10) not null,                      
    active_flag varchar(10) not null,                   -- active flag (indicating current record)
    eff_start_dt timestamp_tz(9) not null,              -- effective start date for scd2
    eff_end_dt timestamp_tz(9),                         -- effective end date for scd2
    current_flag boolean not null default true 
)


comment = 'dimension table for restaurant location'
;


    

merge into 
    consumption_sch.restaurant_location_dim as target 
    using 
        clean_sch.restaurant_location_stm as source
    on 
        target.location_id = source.location_id and
        target.active_flag = source.active_flag
    when matched and 
        source.metadata$action = 'DELETE'and source.metadata$isupdate = 'TRUE'then 
    update set
        target.eff_start_dt = current_timestamp(),
        target.current_flag = False 
    when not matched and 
        source.metadata$action = 'INSERT' and source.metadata$isupdate= 'FALSE' 
    then 

    insert (
        RESTAURANT_LOCATION_HK,
        LOCATION_ID,
        CITY,
        STATE,
        STATE_CODE,
        IS_UNION_TERRITORY,
        CAPITAL_CITY_FLAG,
        CITY_TIER,
        ZIP_CODE,
        ACTIVE_FLAG,
        EFF_START_DT,
        EFF_END_DT,
        CURRENT_FLAG
    )

    values (
        hash(SHA1_hex(CONCAT(source.CITY, source.STATE, source.STATE_CODE, source.ZIP_CODE))),
        source.LOCATION_ID,
        source.CITY,
        source.STATE,
        source.STATE_CODE,
        source.IS_UNION_TERRITORY,
        source.CAPITAL_CITY_FLAG,
        source.CITY_TIER,
        source.ZIP_CODE,
        source.ACTIVE_FLAG,
        CURRENT_TIMESTAMP(),
        NULL,
        TRUE
    );

    select * from consumption_sch.restaurant_location_dim;

    
    
----------------------------------------

copy into stage_sch.location (locationid,city,state,zipcode,activeflag,createdate,modifieddate,
                            _stg_file_name,_stg_file_load_ts,_stg_file_md5,_copy_data_ts)
from (
    select
        t.$1 ::text as locationid,
        t.$2 ::text as city,
        t.$3 ::text as state,
        t.$4 ::text as zipcode,
        t.$5 ::text as activeflag,
        t.$6 ::text as createddate,
        t.$7 ::text as modifieddate,
        metadata$filename as _stg_file_name,
        metadata$file_last_modified as _stg_file_load_ts,
        metadata$file_content_key as _stg_file_md5,
        current_timestamp as _copy_data_ts
        
    from @stage_sch.csv_stage/delta/location/delta-day01-2rows-add.csv t
)
file_format =(format_name= 'stage_sch.csv_file_format')
on_error = abort_statement;
        
    
    







select * from consumption_sch.restaurant_location_dim;


