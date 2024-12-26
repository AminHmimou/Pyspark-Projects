use role sysadmin;
use database sandbox;
use warehouse adho_wh;
use schema stage_sch;


create or replace table stage_sch.customeraddress (
    addressid text,
    customerid text,
    flatno text,
    houseno text,
    floor text,
    building text,
    landmark text,
    locality text,
    city text,
    state text,
    pincode text,
    coordinates text,
    primaryflag text, 
    addresstype text,
    createddate text,
    modifieddate text,

    _stg_file_name text,
    _stg_file_load_ts timestamp,
    _stg_file_md5 text,
    _copy_data_ts timestamp default current_timestamp
);

create or replace stream stage_sch.customeraddress_stm
on table stage_sch.customeraddress
append_only =true
;


copy into stage_sch.customeraddress ( addressid,customerid,flatno,houseno, floor,building,landmark,locality,city,state
                                    ,pincode,coordinates,primaryflag,addresstype,createddate,modifieddate,
                                    _stg_file_name,_stg_file_load_ts,_stg_file_md5, _copy_data_ts)
from (
    select 
        t.$1::text as addressid,
        t.$2::text as customerid,
        t.$3::text as flatno,
        t.$4::text as houseno,
        t.$5::text as floor,
        t.$6::text as building,
        t.$7::text as landmark,
        t.$8::text as locality,
        t.$9::text as city,
        t.$10::text as State,
        t.$11::text as Pincode,
        t.$12::text as coordinates,
        t.$13::text as primaryflag,
        t.$14::text as addresstype,
        t.$15::text as createddate,
        t.$16::text as modifieddate,
        metadata$filename as _stg_file_name,
        metadata$file_last_modified as _stg_file_load_ts,
        metadata$file_content_key as _stg_file_md5,
        current_timestamp as _copy_data_ts

        from @stage_sch.csv_stage/initial/customer_adress/customer_address_book_initial.csv t
        )
file_format = (format_name='stage_sch.csv_file_format')
on_error = abort_statement
;

create or replace table clean_sch.customer_address ( 
    customer_address_sk number autoincrement primary key,
    address_id int ,
    customer_id_fk int,
    flat_no string,
    house_no string,
    floor string, 
    building string,
    landmark string,
    locality string,
    city string,
    state string,
    pincode string,
    coordinates string,
    primary_flag string,
    address_type string,
    created_dt timestamp_tz,
    modified_dt timestamp_tz,
    _stg_file_name string,
    _stg_file_load_ts timestamp,
    _stg_file_md5 string,
    _copy_data_ts timestamp default current_timestamp
    )
    ;


create or replace stream clean_sch.customer_address_stm
on table clean_sch.customer_address;


merge into clean_sch.customer_address as target 
using (
    select 
        cast(addressid as int) as address_id,
        cast(customerid as int) as customer_id_fk,
        flatno as flat_no,
        houseno as house_no,
        floor,
        building,
        landmark,
        locality,
        city,
        state,
        pincode,
        coordinates,
        primaryflag as primary_flag,
        addresstype as address_type,
        try_to_timestamp(createddate, 'YYYY-MM-DD"T"HH24:MI:SS') as created_dt,
        try_to_timestamp(modifieddate, 'YYYY-MM-DD"T"HH24:MI:SS') as modified_dt,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        _copy_data_ts
    from stage_sch.customeraddress_stm
    ) as source

on target.address_id=source.address_id


when matched then
    update set 
        target.flat_no = source.flat_no,
        target.house_no = source.house_no,
        target.floor = source.floor,
        target.building = source.building,
        target.landmark = source.landmark,
        target.locality = source.locality,
        target.city = source.city,
        target.state = source.state,
        target.pincode = source.pincode,
        target.coordinates = source.coordinates,
        target.primary_flag = source.primary_flag,
        target.address_type = source.address_type,
        target.created_dt = source.created_dt,
        target.modified_dt = source.modified_dt,
        target._stg_file_name = source._stg_file_name,
        target._stg_file_load_ts = source._stg_file_load_ts,
        target._stg_file_md5 = source._stg_file_md5,
        target._copy_data_ts = source._copy_data_ts
when not matched then 
    insert (
        address_id,
        customer_id_fk,
        flat_no,
        house_no,
        floor,
        building,
        landmark,
        locality,
        city,
        state,
        pincode,
        coordinates,
        primary_flag,
        address_type,
        created_dt,
        modified_dt,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        _copy_data_ts
    )
    values (
        source.address_id,
        source.customer_id_fk,
        source.flat_no,
        source.house_no,
        source.floor,
        source.building,
        source.landmark,
        source.locality,
        source.city,
        source.state,
        source.pincode,
        source.coordinates,
        source.primary_flag,
        source.address_type,
        source.created_dt,
        source.modified_dt,
        source._stg_file_name,
        source._stg_file_load_ts,
        source._stg_file_md5,
        source._copy_data_ts
    );


create or replace table consumption_sch.customer_address_dim (
    customer_address_hk number primary key,
    address_id int,
    customer_id_fk int, 
    flat_no string,
    house_no string,
    floor string,
    building string,
    LANDMARK STRING,                               -- Landmark
    LOCALITY STRING,                               -- Locality
    CITY STRING,                                   -- City
    STATE STRING,                                  -- State
    PINCODE STRING,                                -- Pincode
    COORDINATES STRING,                            -- Geo-coordinates
    PRIMARY_FLAG STRING,                           -- Whether it's the primary address
    ADDRESS_TYPE STRING,                           -- Type of address (e.g., Home, Office)

    -- SCD2 Columns
    EFF_START_DATE TIMESTAMP_TZ,                                 -- Effective start date
    EFF_END_DATE TIMESTAMP_TZ,                                   -- Effective end date (NULL if active)
    IS_CURRENT BOOLEAN                                           -- Flag to indicate the current record
);


merge into consumption_sch.customer_address_dim as target
using clean_sch.customer_address_stm as source

on 
    target.ADDRESS_ID = source.ADDRESS_ID AND
    target.CUSTOMER_ID_FK = source.CUSTOMER_ID_FK AND
    target.FLAT_NO = source.FLAT_NO AND
    target.HOUSE_NO = source.HOUSE_NO AND
    target.FLOOR = source.FLOOR AND
    target.BUILDING = source.BUILDING AND
    target.LANDMARK = source.LANDMARK AND
    target.LOCALITY = source.LOCALITY AND
    target.CITY = source.CITY AND
    target.STATE = source.STATE AND
    target.PINCODE = source.PINCODE AND
    target.COORDINATES = source.COORDINATES AND
    target.PRIMARY_FLAG = source.PRIMARY_FLAG AND
    target.ADDRESS_TYPE = source.ADDRESS_TYPE
    
when matched  
    and source.metadata$action ='DELETE' and source.metadata$isupdate = 'TRUE' then 
    update set 
        EFF_END_DATE = current_timestamp(),
        IS_CURRENT= FALSE

when not matched 
        and source.metadata$action ='INSERT' and source.metadata$isupdate = 'FALSE' then 
        insert 
        (
            CUSTOMER_ADDRESS_HK,
            ADDRESS_ID,
            CUSTOMER_ID_FK,
            FLAT_NO,
            HOUSE_NO,
            FLOOR,
            BUILDING,
            LANDMARK,
            LOCALITY,
            CITY,
            STATE,
            PINCODE,
            COORDINATES,
            PRIMARY_FLAG,
            ADDRESS_TYPE,
            EFF_START_DATE,
            EFF_END_DATE,
            IS_CURRENT
        )
        values (
            hash(SHA1_hex(CONCAT(source.ADDRESS_ID, source.CUSTOMER_ID_FK, source.FLAT_NO, 
            source.HOUSE_NO, source.FLOOR, source.BUILDING, source.LANDMARK, 
            source.LOCALITY, source.CITY, source.STATE, source.PINCODE, 
            source.COORDINATES, source.PRIMARY_FLAG, source.ADDRESS_TYPE))),
            source.ADDRESS_ID,
            source.CUSTOMER_ID_FK,
            source.FLAT_NO,
            source.HOUSE_NO,
            source.FLOOR,
            source.BUILDING,
            source.LANDMARK,
            source.LOCALITY,
            source.CITY,
            source.STATE,
            source.PINCODE,
            source.COORDINATES,
            source.PRIMARY_FLAG,
            source.ADDRESS_TYPE,
            CURRENT_TIMESTAMP(),
            NULL,
            TRUE
        );

copy into stage_sch.customeraddress (addressid, customerid, flatno, houseno, floor, building, 
                               landmark, locality,city,state,pincode, coordinates, primaryflag, addresstype, 
                               createddate, modifieddate, 
                               _stg_file_name, _stg_file_load_ts, _stg_file_md5, _copy_data_ts)
from (
    select 
        t.$1::text as addressid,
        t.$2::text as customerid,
        t.$3::text as flatno,
        t.$4::text as houseno,
        t.$5::text as floor,
        t.$6::text as building,
        t.$7::text as landmark,
        t.$8::text as locality,
        t.$9::text as city,
        t.$10::text as state,
        t.$11::text as pincode,
        t.$12::text as coordinates,
        t.$13::text as primaryflag,
        t.$14::text as addresstype,
        t.$15::text as createddate,
        t.$16::text as modifieddate,
        metadata$filename as _stg_file_name,
        metadata$file_last_modified as _stg_file_load_ts,
        metadata$file_content_key as _stg_file_md5,
        current_timestamp as _copy_data_ts
    from @stage_sch.csv_stage/delta/customer_adress/day-01-customer-address-book.csv t
)
file_format = (format_name ='stage_sch.csv_file_format')
on_error = abort_statement
;


select * from stage_sch.customeraddress;
select * from stage_sch.customeraddress_stm;
select * from clean_sch.customer_address_dim;
        
copy into stage_sch.customeraddress (addressid, customerid, flatno, houseno, floor, building, 
                               landmark, locality,city,state,pincode, coordinates, primaryflag, addresstype, 
                               createddate, modifieddate, 
                               _stg_file_name, _stg_file_load_ts, _stg_file_md5, _copy_data_ts)
from (
    select 
        t.$1::text as addressid,
        t.$2::text as customerid,
        t.$3::text as flatno,
        t.$4::text as houseno,
        t.$5::text as floor,
        t.$6::text as building,
        t.$7::text as landmark,
        t.$8::text as locality,
        t.$9::text as city,
        t.$10::text as state,
        t.$11::text as pincode,
        t.$12::text as coordinates,
        t.$13::text as primaryflag,
        t.$14::text as addresstype,
        t.$15::text as createddate,
        t.$16::text as modifieddate,
        metadata$filename as _stg_file_name,
        metadata$file_last_modified as _stg_file_load_ts,
        metadata$file_content_key as _stg_file_md5,
        current_timestamp as _copy_data_ts
    from @stage_sch.csv_stage/delta/customer_adress/day-02-customer-address-book.csv t
)
file_format = (format_name ='stage_sch.csv_file_format')
on_error = abort_statement
;


select * from stage_sch.customeraddress;
select * from stage_sch.customeraddress_stm;
select * from clean_sch.customer_address;
select * from consumption_sch.customer_address_dim;