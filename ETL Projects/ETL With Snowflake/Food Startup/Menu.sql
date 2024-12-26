use role sysadmin;
use database sandbox;
use warehouse adho_wh;
use schema stage_sch;


create or replace table stage_sch.menu (
    menuid text,
    restaurantid text,
    itemname text,
    description text,
    price text,
    category text,
    availability text,
    itemtype text,
    createddate text,
    modifieddate text,

    _stg_file_name text,
    _stg_file_load_ts timestamp,
    _stg_file_md5 text,
    _copy_data_ts timestamp default current_timestamp
);

create or replace stream stage_sch.menu_stm
on table stage_sch.menu 
append_only = true ;

copy into stage_sch.menu (menuid,restaurantid,itemname,description,price,category,
                        availability,itemtype,createddate,modifieddate,_stg_file_name,
                        _stg_file_load_ts,_stg_file_md5,_copy_data_ts)
from (
    select 
        t.$1::text as menuid,
        t.$2::text as restaurantid,
        t.$3::text as itemname,
        t.$4::text as description,
        t.$5::text as price,
        t.$6::text as category,
        t.$7::text as availability,
        t.$8::text as itemtype,
        t.$10::text as createddate,
        t.$11::text as modifieddate,
        metadata$filename as _stg_file_name,
        metadata$file_last_modified as _stg_file_load_ts,
        metadata$file_content_key as _stg_file_md5,
        current_timestamp as _copy_data_ts
    from @stage_sch.csv_stage/initial/menu/menu-initial-load.csv t
)
file_format = (format_name ='stage_sch.csv_file_format')
on_error=abort_statement

;

select * from stage_sch.menu_stm;


create or replace table clean_sch.menu (
    menu_sk int autoincrement primary key,
    menu_id int not null unique,
    restaurant_id_fk int ,
    item_name string not null,
    description string not null,
    price decimal(10,2) not null,
    category string ,
    availability string,
    item_type string,
    created_dt timestamp_ntz,
    modified_dt timestamp_ntz,

    _stg_file_name string,
    _stg_file_load_ts timestamp_ntz,
    _stg_file_md5 string,
    _copy_data_ts timestamp_ntz default current_timestamp

);

create or replace stream clean_sch.menu_stm
on table clean_sch.menu;


merge into clean_sch.menu as target 
using (
    select 
        try_cast(menuid as int) as menu_id,
        try_cast(restaurantid as int) as restaurant_id_fk,
        Trim(itemname) as Item_Name,
        Trim(description) as Description,
        try_cast(price as decimal(10,2)) as price,
        Trim(category) as Category,
        case 
            when lower(availability)='true' then TRUE
            when lower(availability)='false' then FALSE
            else null
        end as Availability,
        trim(itemtype) as Item_Type,
        try_cast(createddate as timestamp_ntz) as Created_dt,
        try_cast(modifieddate as timestamp_ntz) as Modified_dt,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        _copy_data_ts
        from stage_sch.menu_stm
) as source 

On target.menu_id = source.menu_id 
when matched then 
    update set 
        target.Restaurant_ID_FK = source.Restaurant_ID_FK,
        target.Item_Name = source.Item_Name,
        target.Description = source.Description,
        target.Price = source.Price,
        target.Category = source.Category,
        target.Availability = source.Availability,
        target.Item_Type = source.Item_Type,
        target.Created_dt = source.Created_dt,  
        target.Modified_dt = source.Modified_dt,  
        target._STG_FILE_NAME = source._stg_file_name,
        target._STG_FILE_LOAD_TS = source._stg_file_load_ts,
        target._STG_FILE_MD5 = source._stg_file_md5,
        target._COPY_DATA_TS = CURRENT_TIMESTAMP
when not matched then
    insert (
        Menu_ID,
        Restaurant_ID_FK,
        Item_Name,
        Description,
        Price,
        Category,
        Availability,
        Item_Type,
        Created_dt, 
        Modified_dt,  
        _STG_FILE_NAME,
        _STG_FILE_LOAD_TS,
        _STG_FILE_MD5,
        _COPY_DATA_TS
    ) values (
        source.Menu_ID,
        source.Restaurant_ID_FK,
        source.Item_Name,
        source.Description,
        source.Price,
        source.Category,
        source.Availability,
        source.Item_Type,
        source.Created_dt,  
        source.Modified_dt,  
        source._stg_file_name,
        source._stg_file_load_ts,
        source._stg_file_md5,
        CURRENT_TIMESTAMP
    );

create or replace table consumption_sch.menu_dim (
    menu_dim_hk number primary key,
    menu_id int not null,
    restaurant_id_fk int not null,
    item_name string,
    Description string,
    Price decimal(10,2),
    Category string,
    Availability boolean,
    Item_Type string,
    eff_start_date timestamp_ntz,
    eff_end_date timestamp,
    is_current boolean
)
;

merge into consumption_sch.menu_dim as target 
using clean_sch.menu_stm as source
on  
    target.Menu_ID = source.Menu_ID AND
    target.Restaurant_ID_FK = source.Restaurant_ID_FK AND
    target.Item_Name = source.Item_Name AND
    target.Description = source.Description AND
    target.Price = source.Price AND
    target.Category = source.Category AND
    target.Availability = source.Availability AND
    target.Item_Type = source.Item_Type
when matched 
    and source.metadata$action = 'DELETE'
    and source.metadata$isupdate = 'TRUE' then 
    update set 
        target.eff_end_date = current_timestamp(),
        target.is_current = FALSE
when not matched 
    and source.metadata$action = 'INSERT'
    and source.metadata$isupdate = 'FALSE' then 
    insert 
        (
        Menu_Dim_HK,               -- Hash key
        Menu_ID,
        Restaurant_ID_FK,
        Item_Name,
        Description,
        Price,
        Category,
        Availability,
        Item_Type,
        EFF_START_DATE,
        EFF_END_DATE,
        IS_CURRENT
        )
    values (
        hash(SHA1_hex(CONCAT(source.Menu_ID, source.Restaurant_ID_FK, 
            source.Item_Name, source.Description, source.Price, 
            source.Category, source.Availability, source.Item_Type))),  -- Hash key
        source.Menu_ID,
        source.Restaurant_ID_FK,
        source.Item_Name,
        source.Description,
        source.Price,
        source.Category,
        source.Availability,
        source.Item_Type,
        CURRENT_TIMESTAMP(),       -- Effective start date
        NULL,                      -- Effective end date (NULL for current record)
        TRUE           
        );

----copy some data into the stage table
copy into stage_sch.menu (menuid, restaurantid, itemname, description, price, category, 
                availability, itemtype, createddate, modifieddate,
                _stg_file_name, _stg_file_load_ts, _stg_file_md5, _copy_data_ts)
from (
    select 
        t.$1::text as menuid,
        t.$2::text as restaurantid,
        t.$3::text as itemname,
        t.$4::text as description,
        t.$5::text as price,
        t.$6::text as category,
        t.$7::text as availability,
        t.$8::text as itemtype,
        t.$9::text as createddate,
        t.$10::text as modifieddate,
        metadata$filename as _stg_file_name,
        metadata$file_last_modified as _stg_file_load_ts,
        metadata$file_content_key as _stg_file_md5,
        current_timestamp as _copy_data_ts
    from @stage_sch.csv_stage/delta/menu/day-01-menu-data.csv t
)
file_format = (format_name = 'stage_sch.csv_file_format')
on_error = abort_statement;

-------add some new data 


copy into stage_sch.menu (menuid, restaurantid, itemname, description, price, category, 
                availability, itemtype, createddate, modifieddate,
                _stg_file_name, _stg_file_load_ts, _stg_file_md5, _copy_data_ts)
from (
    select 
        t.$1::text as menuid,
        t.$2::text as restaurantid,
        t.$3::text as itemname,
        t.$4::text as description,
        t.$5::text as price,
        t.$6::text as category,
        t.$7::text as availability,
        t.$8::text as itemtype,
        t.$9::text as createddate,
        t.$10::text as modifieddate,
        metadata$filename as _stg_file_name,
        metadata$file_last_modified as _stg_file_load_ts,
        metadata$file_content_key as _stg_file_md5,
        current_timestamp as _copy_data_ts
    from @stage_sch.csv_stage/delta/menu/day-02-menu-data.csv t
)
file_format = (format_name = 'stage_sch.csv_file_format')
on_error = abort_statement;


select * from consumption_sch.menu_dim;