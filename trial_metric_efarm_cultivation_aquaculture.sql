with 
filter_account_prod as 
(
select leads_id 
from 
{{ source('staging','stg_cultivation_users') }}
where (created_by
in (
'bavian.adi@efishery.com'
,'maq.riki@efishery.com'
,'rangga.luthanza@efishery.com'
,'tubagus.assad@efishery.com'
,'ade.yudha@efishery.com'
,'althof.ramdhan@efishery.com'
,'arief.yusron@efishery.com'
,'muhammad.aditya@efishery.com'
,'doni.pebruwantoro@efishery.com'
,'wihlarko.prasdegho@efishery.com'
,'andriannus.parasian@efishery.com'
,'rachmat.adi@efishery.com'
,'riko.pernando@efishery.com'
,'roy.parsaoran@efishery.com'
,'m.sholeh@efishery.com'
,'elsa.vinietta@efishery.com'
-- ,'azrilhanif.revrani@efishery.com'
)
or leads_id in ('512nznbdjmfa','512h62qp59n9'))
and leads_id not in ('512eypdm5ks9'
,'512h2f4fm42j', '5129jmjcva9p') --REVAMP ADD 5129jmjcva9p
)
,email_prod as 
(
select distinct created_by 
from 
{{ source('staging','stg_cultivation_cycles') }}
where created_by
in (
'bavian.adi@efishery.com'
,'maq.riki@efishery.com'
,'rangga.luthanza@efishery.com'
,'tubagus.assad@efishery.com'
,'ade.yudha@efishery.com'
,'althof.ramdhan@efishery.com'
,'arief.yusron@efishery.com'
,'muhammad.aditya@efishery.com'
,'doni.pebruwantoro@efishery.com'
,'wihlarko.prasdegho@efishery.com'
,'andriannus.parasian@efishery.com'
,'rachmat.adi@efishery.com'
,'riko.pernando@efishery.com'
,'roy.parsaoran@efishery.com'
,'m.sholeh@efishery.com'
,'elsa.vinietta@efishery.com'
-- ,'azrilhanif.revrani@efishery.com'
)
)
,t_base_action_cultivation as 
(
    select 
        distinct cycle_uuid,action_date date_key,data_collection_method,created_by,contributors,appsource
    from 
        {{ source('staging','stg_cultivation_harvests') }}
    where cycle_uuid is not null and cycle_uuid !='' and deleted_at is null and created_by not in ('bavian.adi@efishery.com','rangga.luthanza@efishery.com','soni.adiyatma@efishery.com')
    and created_by not in (select created_by from email_prod) 
    union all
    select 
        distinct cycle_uuid,action_date date_key,data_collection_method,created_by,contributors,appsource
    from 
        {{ source('staging','stg_cultivation_feeds') }}
    where cycle_uuid is not null and cycle_uuid !='' and deleted_at is null and created_by not in ('bavian.adi@efishery.com','rangga.luthanza@efishery.com','soni.adiyatma@efishery.com')
    and created_by not in (select created_by from email_prod) 
    union all
    select 
        distinct cycle_uuid,action_date date_key,data_collection_method,created_by,contributors,appsource
    from 
        {{ source('staging','stg_cultivation_bacterias') }}
    where cycle_uuid is not null and cycle_uuid !='' and deleted_at is null and created_by not in ('bavian.adi@efishery.com','rangga.luthanza@efishery.com','soni.adiyatma@efishery.com')
    and created_by not in (select created_by from email_prod) 
    union all
    select 
        distinct cycle_uuid,action_date date_key,data_collection_method,created_by,contributors,appsource
    from 
        {{ source('staging','stg_cultivation_diseases') }}
    where cycle_uuid is not null and cycle_uuid !='' and deleted_at is null and created_by not in ('bavian.adi@efishery.com','rangga.luthanza@efishery.com','soni.adiyatma@efishery.com')
    and created_by not in (select created_by from email_prod) 
    union all
    select 
        distinct cycle_uuid,action_date date_key,data_collection_method,created_by,contributors,appsource
    from 
        {{ source('staging','stg_cultivation_planktons') }}
    where cycle_uuid is not null and cycle_uuid !='' and deleted_at is null and created_by not in ('bavian.adi@efishery.com','rangga.luthanza@efishery.com','soni.adiyatma@efishery.com')
    and created_by not in (select created_by from email_prod) 
    union all
    select 
        distinct cycle_uuid,action_date date_key,data_collection_method,created_by,contributors,appsource
    from 
        {{ source('staging','stg_cultivation_samplings') }}
    where cycle_uuid is not null and cycle_uuid !='' and deleted_at is null and created_by not in ('bavian.adi@efishery.com','rangga.luthanza@efishery.com','soni.adiyatma@efishery.com')
    and created_by not in (select created_by from email_prod) 
    union all
    select 
        distinct cycle_uuid,action_date date_key,data_collection_method,created_by,contributors,appsource
    from 
        {{ source('staging','stg_cultivation_water_chemicals') }}
    where cycle_uuid is not null and cycle_uuid !='' and deleted_at is null and created_by not in ('bavian.adi@efishery.com','rangga.luthanza@efishery.com','soni.adiyatma@efishery.com')
    and created_by not in (select created_by from email_prod) 
    union all
    select 
        distinct cycle_uuid,action_date date_key,data_collection_method,created_by,contributors,appsource
    from 
        {{ source('staging','stg_cultivation_water_times') }}
    where cycle_uuid is not null and cycle_uuid !='' and deleted_at is null and created_by not in ('bavian.adi@efishery.com','rangga.luthanza@efishery.com','soni.adiyatma@efishery.com')
    and created_by not in (select created_by from email_prod) 
    union all 
    select 
        distinct cycle_uuid,action_date date_key,data_collection_method,created_by,contributors,appsource
    from 
        {{ source('staging','stg_cultivation_water_conditionings') }}
    where cycle_uuid is not null and cycle_uuid !='' and deleted_at is null and created_by not in ('bavian.adi@efishery.com','rangga.luthanza@efishery.com','soni.adiyatma@efishery.com')
    and created_by not in (select created_by from email_prod) 
    union all
    select 
        distinct cycle_uuid,action_date date_key,data_collection_method,created_by,contributors,appsource
    from 
        {{ source('staging','stg_cultivation_hepatopancreas') }}
    where cycle_uuid is not null and cycle_uuid !='' and deleted_at is null and created_by not in ('bavian.adi@efishery.com','rangga.luthanza@efishery.com','soni.adiyatma@efishery.com')
    and created_by not in (select created_by from email_prod) 
    union all 
    select 
        distinct cycle_uuid,action_date date_key,data_collection_method,created_by,contributors,appsource
    from 
        {{ source('staging','stg_cultivation_intestines') }}
    where cycle_uuid is not null and cycle_uuid !='' and deleted_at is null and created_by not in ('bavian.adi@efishery.com','rangga.luthanza@efishery.com','soni.adiyatma@efishery.com')
    and created_by not in (select created_by from email_prod) 
)
,base_action_cultivation as
(
    select 
        cycle_uuid
        ,date_key
        ,string_agg(distinct contributors,', ') as contributors
        ,string_agg(distinct case when appsource is not null and appsource!='' then appsource end, ', ') appsource
        ,string_agg(distinct case when data_collection_method is not null and data_collection_method!='' then data_collection_method end, ', ') data_collection_method
    from t_base_action_cultivation 
    left join unnest(contributors) contributors on true
    group by 1,2
)
-- water start
,t_water_chemicals as
(
    select 
        cycle_uuid,
        action_date AS date_key,
        created_by,
        'all' AS type_time,
        CAST(NULL AS STRING) AS weather,
        CAST(NULL AS STRING) AS water_colour,
        AVG(CAST(tan AS NUMERIC)) AS tan,
        AVG(CAST(tom AS NUMERIC)) AS tom,
        AVG(CAST(tss AS NUMERIC)) AS tss,
        AVG(CAST(ci AS NUMERIC)) AS ci,
        AVG(CAST(orp AS NUMERIC)) AS orp,
        AVG(CAST(no2 AS NUMERIC)) AS no2,
        AVG(CAST(no3 AS NUMERIC)) AS no3,
        AVG(CAST(water_hardness AS NUMERIC)) AS water_hardness,
        AVG(CAST(nh3 AS NUMERIC)) AS nh3,
        AVG(CAST(nh4 AS NUMERIC)) AS nh4,
        AVG(CAST(fe AS NUMERIC)) AS fe,
        AVG(CAST(po4 AS NUMERIC)) AS po4,
        AVG(CAST(ca AS NUMERIC)) AS ca,
        AVG(CAST(mg AS NUMERIC)) AS mg,
        AVG(CAST(co3 AS NUMERIC)) AS co3,
        AVG(CAST(hco3 AS NUMERIC)) AS hco3,
        AVG(CAST(alkalinity_total AS NUMERIC)) AS alkalinity_total,
        AVG(CAST(brightness AS NUMERIC)) AS brightness,
        AVG(CAST(cod AS NUMERIC)) AS cod,
        CAST(NULL AS NUMERIC) AS temperature,
        CAST(NULL AS NUMERIC) AS ph,
        CAST(NULL AS NUMERIC) AS value_do,
        CAST(NULL AS NUMERIC) AS kincir,
        CAST(NULL AS NUMERIC) AS water_height,
        CAST(NULL AS NUMERIC) AS tds,
        CAST(NULL AS NUMERIC) AS water_clarity,
        CAST(NULL AS NUMERIC) AS salinity
    from 
        {{ source('staging','stg_cultivation_water_chemicals')}}
    where cycle_uuid is not null and deleted_at is null
    and created_by
    not in (
    'bavian.adi@efishery.com'
    ,'maq.riki@efishery.com'
    ,'rangga.luthanza@efishery.com'
    ,'tubagus.assad@efishery.com'
    ,'ade.yudha@efishery.com'
    ,'althof.ramdhan@efishery.com'
    ,'arief.yusron@efishery.com'
    ,'muhammad.aditya@efishery.com'
    ,'doni.pebruwantoro@efishery.com'
    ,'wihlarko.prasdegho@efishery.com'
    ,'andriannus.parasian@efishery.com'
    ,'rachmat.adi@efishery.com'
    ,'riko.pernando@efishery.com'
    ,'roy.parsaoran@efishery.com'
    ,'m.sholeh@efishery.com'
    ,'elsa.vinietta@efishery.com')
    group by 1,2,3
)
,water_times as 
(
    select 
        cycle_uuid,
        action_date AS date_key,
        created_by,
        CASE 
            WHEN LOWER(type) IS NULL OR LOWER(type) = '' THEN 'all' 
            ELSE LOWER(type) 
        END AS type_time,

        STRING_AGG(DISTINCT CASE 
                    WHEN weather IS NOT NULL AND weather != '' 
                    THEN LOWER(weather) 
                    END, ', ') AS weather,

        REPLACE(
            STRING_AGG(DISTINCT CASE 
                        WHEN water_colour IS NOT NULL AND water_colour != '' 
                        THEN LOWER(water_colour) 
                        END, ', '), 
            '_', ' ') AS water_colour,

        CAST(NULL AS NUMERIC) AS tan,
        CAST(NULL AS NUMERIC) AS tom,
        CAST(NULL AS NUMERIC) AS tss,
        CAST(NULL AS NUMERIC) AS ci,
        AVG(CAST(orp AS NUMERIC)) AS orp,
        CAST(NULL AS NUMERIC) AS no2,
        CAST(NULL AS NUMERIC) AS no3,
        CAST(NULL AS NUMERIC) AS water_hardness,
        CAST(NULL AS NUMERIC) AS nh3,
        CAST(NULL AS NUMERIC) AS nh4,
        CAST(NULL AS NUMERIC) AS fe,
        CAST(NULL AS NUMERIC) AS po4,
        CAST(NULL AS NUMERIC) AS ca,
        CAST(NULL AS NUMERIC) AS mg,
        CAST(NULL AS NUMERIC) AS co3,
        CAST(NULL AS NUMERIC) AS hco3,
        CAST(NULL AS NUMERIC) AS alkalinity_total,
        CAST(NULL AS NUMERIC) AS brightness,
        CAST(NULL AS NUMERIC) AS cod,

        AVG(CAST(temperature AS NUMERIC)) AS temperature,
        AVG(CAST(ph AS NUMERIC)) AS ph,
        AVG(CAST(do AS NUMERIC)) AS value_do,  -- Use backticks for reserved keywords like "do"
        AVG(CAST(kincir AS NUMERIC)) AS kincir,
        AVG(CAST(water_height AS NUMERIC)) AS water_height,
        AVG(CAST(tds AS NUMERIC)) AS tds,
        AVG(CAST(water_clarity AS NUMERIC)) AS water_clarity,
        AVG(CAST(salinity AS NUMERIC)) AS salinity
    from 
        {{ source('staging','stg_cultivation_water_times') }}
    where cycle_uuid is not null and deleted_at is null
    and created_by
    not in (
    'bavian.adi@efishery.com'
    ,'maq.riki@efishery.com'
    ,'rangga.luthanza@efishery.com'
    ,'tubagus.assad@efishery.com'
    ,'ade.yudha@efishery.com'
    ,'althof.ramdhan@efishery.com'
    ,'arief.yusron@efishery.com'
    ,'muhammad.aditya@efishery.com'
    ,'doni.pebruwantoro@efishery.com'
    ,'wihlarko.prasdegho@efishery.com'
    ,'andriannus.parasian@efishery.com'
    ,'rachmat.adi@efishery.com'
    ,'riko.pernando@efishery.com'
    ,'roy.parsaoran@efishery.com'
    ,'m.sholeh@efishery.com'
    ,'elsa.vinietta@efishery.com')
    group by 1,2,3,4
)
,water_all as 
(
    select * from water_times
    union all
    select * from t_water_chemicals
)
,summary_waters as 
(
    select 
        cycle_uuid,
        date_key,
        string_agg(distinct case when created_by is not null and created_by!='' then created_by end, ', ') created_by,
        string_agg(distinct case when weather is not null and weather!='' then lower(weather) end, ', ') weather,
        string_agg(distinct case when water_colour is not null and water_colour !='' then lower(water_colour) end, ', ') water_colour,
        avg(tan) tan_all,
        avg(tom) tom_all,
        avg(tss) tss_all,
        avg(ci) ci_all,
        avg(orp) orp_all,
        avg(no2) no2_all,
        avg(no3) no3_all,
        avg(water_hardness) water_hardness_all,
        avg(nh3) nh3_all,
        avg(nh4) nh4_all,
        avg(fe) fe_all,
        avg(po4) po4_all,
        avg(ca) ca_all,
        avg(mg) mg_all,
        avg(co3) co3_all,
        avg(hco3) hco3_all,
        avg(alkalinity_total) alkalinity_total_all,
        avg(brightness) brightness_all,
        avg(cod) cod_all,
        avg(temperature) temperature_all,
        avg(ph) ph_all,
        avg(value_do) value_do_all,
        avg(kincir) kincir_all,
        avg(water_height) water_height_all,
        avg(tds) tds_all,
        avg(water_clarity) water_clarity_all,
        avg(salinity) salinity,
        string_agg(distinct case when weather is not null and weather!='' and type_time='pagi' then lower(weather) end, ',') weather_pagi,
        string_agg(distinct case when water_colour is not null and water_colour !='' and type_time='pagi' then lower(water_colour) end, ',') water_colour_pagi,
        avg(case when type_time='pagi' then tan end) tan_pagi,
        avg(case when type_time='pagi' then tom end) tom_pagi,
        avg(case when type_time='pagi' then tss end) tss_pagi,
        avg(case when type_time='pagi' then ci end) ci_pagi,
        avg(case when type_time='pagi' then orp end) orp_pagi,
        avg(case when type_time='pagi' then no2 end) no2_pagi,
        avg(case when type_time='pagi' then no3 end) no3_pagi,
        avg(case when type_time='pagi' then water_hardness end) water_hardness_pagi,
        avg(case when type_time='pagi' then nh3 end) nh3_pagi,
        avg(case when type_time='pagi' then nh4 end) nh4_pagi,
        avg(case when type_time='pagi' then fe end) fe_pagi,
        avg(case when type_time='pagi' then po4 end) po4_pagi,
        avg(case when type_time='pagi' then ca end) ca_pagi,
        avg(case when type_time='pagi' then mg end) mg_pagi,
        avg(case when type_time='pagi' then co3 end) co3_pagi,
        avg(case when type_time='pagi' then hco3 end) hco3_pagi,
        avg(case when type_time='pagi' then alkalinity_total end) alkalinity_total_pagi,
        avg(case when type_time='pagi' then brightness end) brightness_pagi,
        avg(case when type_time='pagi' then cod end) cod_pagi,
        avg(case when type_time='pagi' then temperature end) temperature_pagi,
        avg(case when type_time='pagi' then ph end) ph_pagi,
        avg(case when type_time='pagi' then value_do end) value_do_pagi,
        avg(case when type_time='pagi' then kincir end) kincir_pagi,
        avg(case when type_time='pagi' then water_height end) water_height_pagi,
        avg(case when type_time='pagi' then tds end) tds_pagi,
        avg(case when type_time='pagi' then water_clarity end) water_clarity_pagi,
        avg(case when type_time='pagi' then salinity end) salinity_pagi,
        string_agg(distinct case when weather is not null and weather!='' and type_time='sore' then lower(weather) end, ',') weather_sore,
        string_agg(distinct case when water_colour is not null and water_colour !='' and type_time='sore' then lower(water_colour) end, ',') water_colour_sore,
        avg(case when type_time='sore' then tan end) tan_sore,
        avg(case when type_time='sore' then tom end) tom_sore,
        avg(case when type_time='sore' then tss end) tss_sore,
        avg(case when type_time='sore' then ci end) ci_sore,
        avg(case when type_time='sore' then orp end) orp_sore,
        avg(case when type_time='sore' then no2 end) no2_sore,
        avg(case when type_time='sore' then no3 end) no3_sore,
        avg(case when type_time='sore' then water_hardness end) water_hardness_sore,
        avg(case when type_time='sore' then nh3 end) nh3_sore,
        avg(case when type_time='sore' then nh4 end) nh4_sore,
        avg(case when type_time='sore' then fe end) fe_sore,
        avg(case when type_time='sore' then po4 end) po4_sore,
        avg(case when type_time='sore' then ca end) ca_sore,
        avg(case when type_time='sore' then mg end) mg_sore,
        avg(case when type_time='sore' then co3 end) co3_sore,
        avg(case when type_time='sore' then hco3 end) hco3_sore,
        avg(case when type_time='sore' then alkalinity_total end) alkalinity_total_sore,
        avg(case when type_time='sore' then brightness end) brightness_sore,
        avg(case when type_time='sore' then cod end) cod_sore,
        avg(case when type_time='sore' then temperature end) temperature_sore,
        avg(case when type_time='sore' then ph end) ph_sore,
        avg(case when type_time='sore' then value_do end) value_do_sore,
        avg(case when type_time='sore' then kincir end) kincir_sore,
        avg(case when type_time='sore' then water_height end) water_height_sore,
        avg(case when type_time='sore' then tds end) tds_sore,
        avg(case when type_time='sore' then water_clarity end) water_clarity_sore,
        avg(case when type_time='sore' then salinity end) salinity_sore,
        string_agg(distinct case when weather is not null and weather!='' and type_time='malam' then lower(weather) end, ',') weather_malam,
        string_agg(distinct case when water_colour is not null and water_colour !='' and type_time='malam' then lower(water_colour) end, ',') water_colour_malam,
        avg(case when type_time='malam' then tan end) tan_malam,
        avg(case when type_time='malam' then tom end) tom_malam,
        avg(case when type_time='malam' then tss end) tss_malam,
        avg(case when type_time='malam' then ci end) ci_malam,
        avg(case when type_time='malam' then orp end) orp_malam,
        avg(case when type_time='malam' then no2 end) no2_malam,
        avg(case when type_time='malam' then no3 end) no3_malam,
        avg(case when type_time='malam' then water_hardness end) water_hardness_malam,
        avg(case when type_time='malam' then nh3 end) nh3_malam,
        avg(case when type_time='malam' then nh4 end) nh4_malam,
        avg(case when type_time='malam' then fe end) fe_malam,
        avg(case when type_time='malam' then po4 end) po4_malam,
        avg(case when type_time='malam' then ca end) ca_malam,
        avg(case when type_time='malam' then mg end) mg_malam,
        avg(case when type_time='malam' then co3 end) co3_malam,
        avg(case when type_time='malam' then hco3 end) hco3_malam,
        avg(case when type_time='malam' then alkalinity_total end) alkalinity_total_malam,
        avg(case when type_time='malam' then brightness end) brightness_malam,
        avg(case when type_time='malam' then cod end) cod_malam,
        avg(case when type_time='malam' then temperature end) temperature_malam,
        avg(case when type_time='malam' then ph end) ph_malam,
        avg(case when type_time='malam' then value_do end) value_do_malam,
        avg(case when type_time='malam' then kincir end) kincir_malam,
        avg(case when type_time='malam' then water_height end) water_height_malam,
        avg(case when type_time='malam' then tds end) tds_malam,
        avg(case when type_time='malam' then water_clarity end) water_clarity_malam,
        avg(case when type_time='malam' then salinity end) salinity_malam
    from water_all   
    group by 1,2
)
-- water end
-- water conditioning start
,summary_water_conditioning as 
(
select 
    cycle_uuid
    ,action_date date_key
    ,string_agg(distinct case when is_not_siphon is not null then cast(is_not_siphon as string) end, ',') is_siphon
    ,string_agg(distinct case when is_over_probiotik is not null then cast(is_over_probiotik as string) end, ',') is_over_probiotik
    ,string_agg(distinct case when is_over_feed is not null then cast(is_over_feed as string) end, ',') is_over_feed
    ,string_agg(distinct case when is_water_not_circulated is not null then cast(is_water_not_circulated as string) end, ',') is_water_circulated
    ,string_agg(distinct case when season is not null then cast(season as string) end, ',') season
from 
    {{ source('staging','stg_cultivation_water_conditionings') }}
where 
    deleted_at is null and cycle_uuid is not null
    and 
    (is_not_siphon is not null or is_over_probiotik is not null
    or is_over_feed  is not null or is_water_not_circulated  is not null or season  is not null)
    and created_by
    not in (
    'bavian.adi@efishery.com'
    ,'maq.riki@efishery.com'
    ,'rangga.luthanza@efishery.com'
    ,'tubagus.assad@efishery.com'
    ,'ade.yudha@efishery.com'
    ,'althof.ramdhan@efishery.com'
    ,'arief.yusron@efishery.com'
    ,'muhammad.aditya@efishery.com'
    ,'doni.pebruwantoro@efishery.com'
    ,'wihlarko.prasdegho@efishery.com'
    ,'andriannus.parasian@efishery.com'
    ,'rachmat.adi@efishery.com'
    ,'riko.pernando@efishery.com'
    ,'roy.parsaoran@efishery.com'
    ,'m.sholeh@efishery.com'
    ,'elsa.vinietta@efishery.com')
group by 1,2
)
-- water conditioning end
-- feeding start
,t_summary_feeding as (
    select 
        a.cycle_uuid,a.uuid feeding_uuid,a.action_date action_time_feeding,a.feed_size,a.feed_weight,
        case when lower(a.feeding_type)='fr' then 'FR'
        when lower(a.feeding_type) in ('index','indeks') then 'INDEX'
        else 'Others' end feeding_type_category
        ,a.feeding_type_value
        ,a.cumulative_feed_weight
        ,sum(cast(b.amount as decimal)) as total_pakan
        ,string_agg(distinct case when c.name is not null then name end, ',') feed_name_combination,max(b.iteration) max_iteration
    from 
        {{ source('staging','stg_cultivation_feeds') }} a 
    left join 
    {{ source('staging','stg_cultivation_feedings') }} b on a.uuid=b.feed_uuid
    left join 
    {{ source('staging','stg_cultivation_products_feeds') }} c on c.uuid=b.feed_product_uuid 
    where 
        cycle_uuid is not null and a.deleted_at is null
        and a.created_by
        not in (
        'bavian.adi@efishery.com'
        ,'maq.riki@efishery.com'
        ,'rangga.luthanza@efishery.com'
        ,'tubagus.assad@efishery.com'
        ,'ade.yudha@efishery.com'
        ,'althof.ramdhan@efishery.com'
        ,'arief.yusron@efishery.com'
        ,'muhammad.aditya@efishery.com'
        ,'doni.pebruwantoro@efishery.com'
        ,'wihlarko.prasdegho@efishery.com'
        ,'andriannus.parasian@efishery.com'
        ,'rachmat.adi@efishery.com'
        ,'riko.pernando@efishery.com'
        ,'roy.parsaoran@efishery.com'
        ,'m.sholeh@efishery.com'
        ,'elsa.vinietta@efishery.com')
    group by 1,2,3,4,5,6,7,8 
)
,summary_feedings as (
    select 
        cycle_uuid,date(action_time_feeding) date_key
        ,string_agg(distinct case when feeding_type_category is not null then feeding_type_category end, ',') feeding_type_category
        ,avg(case when feed_size = '' then 0 else cast(feed_size as numeric) end) feed_size
        ,avg(case when feed_weight = '' then 0 else cast(feed_weight as numeric) end) feed_weight
        ,avg((case when feeding_type_value = '' then 0 else cast(feeding_type_value as numeric) end)/100) feeding_type_value
        ,avg(case when cumulative_feed_weight = '' then 0 else cast(cumulative_feed_weight as numeric) end) cumulative_feed_weight
        ,avg(total_pakan) total_pakan
        ,string_agg(distinct case when feed_name_combination is not null then feed_name_combination end, ',')feed_name_combination
    from t_summary_feeding
    group by 1,2
)
,summary_samplings as (
    select
        cycle_uuid
        ,date_key 
        ,avg(cast(mbw as numeric)) mbw
        ,avg(cast(shrimp_length as numeric)) shrimp_length
        ,avg(cast(fr as numeric)) fr
        ,string_agg(distinct cast(case when is_healthy is not null then is_healthy end as string), ',') is_healthy
        ,string_agg(distinct case when commodity_gills is not null then commodity_gills end, ',') commodity_gills
        ,string_agg(distinct case when commodity_swim_legs is not null then commodity_swim_legs end, ',') commodity_swim_legs
        ,string_agg(distinct case when commodity_intestine_colour is not null then commodity_intestine_colour end, ',') commodity_intestine_colour
        ,string_agg(distinct case when commodity_tail is not null then commodity_tail end, ',') commodity_tail
        ,string_agg(distinct case when commodity_hepato is not null then commodity_hepato end, ',') commodity_hepato
        ,string_agg(distinct case when commodity_colour is not null then commodity_colour end, ',') commodity_colour
        ,string_agg(distinct case when commodity_shell is not null then commodity_shell end, ',') commodity_shell
        ,string_agg(distinct case when commodity_flesh is not null then commodity_flesh end, ',') commodity_flesh
        ,string_agg(distinct cast(case when is_diseased is not null then is_diseased end as string), ',') is_diseased
        ,string_agg(distinct case when disease is not null then disease end, ',') disease
    from
    (
        select 
            a.cycle_uuid
            ,a.action_date date_key 
            ,b.mbw 
            ,b.fr 
            ,b.shrimp_length 
            ,b.is_healthy 
            ,a.commodity_gills -- deprecated soon!
            ,a.commodity_swim_legs -- deprecated soon!
            ,a.commodity_intestine_colour -- deprecated soon!
            ,a.commodity_tail -- deprecated soon!
            ,a.commodity_hepato -- deprecated soon! 
            ,a.commodity_colour -- deprecated soon!
            ,a.commodity_shell -- deprecated soon!
            ,a.commodity_flesh -- deprecated soon!
            ,a.is_diseased -- deprecated soon!
            ,a.disease -- deprecated soon!
            ,row_number()over(partition by a.cycle_uuid,a.action_date order by a.created_at desc) rn
        from {{ source('staging','stg_cultivation_samplings') }} a
        left join {{ source('staging','stg_cultivation_sampling_details') }} b on a.uuid = b.sampling_uuid
        where a.deleted_at is null and a.cycle_uuid is not null  
        and b.deleted_at is null
        and a.created_by
        not in (
        'bavian.adi@efishery.com'
        ,'maq.riki@efishery.com'
        ,'rangga.luthanza@efishery.com'
        ,'tubagus.assad@efishery.com'
        ,'ade.yudha@efishery.com'
        ,'althof.ramdhan@efishery.com'
        ,'arief.yusron@efishery.com'
        ,'muhammad.aditya@efishery.com'
        ,'doni.pebruwantoro@efishery.com'
        ,'wihlarko.prasdegho@efishery.com'
        ,'andriannus.parasian@efishery.com'
        ,'rachmat.adi@efishery.com'
        ,'riko.pernando@efishery.com'
        ,'roy.parsaoran@efishery.com'
        ,'m.sholeh@efishery.com'
        ,'elsa.vinietta@efishery.com')
    ) t
    where rn=1
    group by 1,2
)
,summary_planktons as (
    select
        cycle_uuid
        ,action_date date_key
        ,avg(cast(ratio_green_algae as numeric)) ratio_green_algae
        ,avg(cast(ratio_blue_green_algae as numeric)) ratio_blue_green_algae
        ,avg(cast(ratio_golden_green_algae as numeric)) ratio_golden_green_algae
        ,avg(cast(ratio_diatom as numeric)) ratio_diatom
        ,avg(cast(ratio_euglenophyta as numeric)) ratio_euglenophyta
        ,avg(cast(ratio_dinoflagellata as numeric)) ratio_dinoflagelata
        ,avg(cast(ratio_protozoa_zooplankton as numeric)) ratio_protozoa_zooplankton
        ,sum(cast(total_plankton as numeric)) total_plankton
        ,count(distinct uuid) total_input_planktons
    from 
        {{ source('staging','stg_cultivation_planktons') }}
    where 
        deleted_at is null and cycle_uuid is not null
        and created_by
        not in (
        'bavian.adi@efishery.com'
        ,'maq.riki@efishery.com'
        ,'rangga.luthanza@efishery.com'
        ,'tubagus.assad@efishery.com'
        ,'ade.yudha@efishery.com'
        ,'althof.ramdhan@efishery.com'
        ,'arief.yusron@efishery.com'
        ,'muhammad.aditya@efishery.com'
        ,'doni.pebruwantoro@efishery.com'
        ,'wihlarko.prasdegho@efishery.com'
        ,'andriannus.parasian@efishery.com'
        ,'rachmat.adi@efishery.com'
        ,'riko.pernando@efishery.com'
        ,'roy.parsaoran@efishery.com'
        ,'m.sholeh@efishery.com'
        ,'elsa.vinietta@efishery.com')
    group by 1,2
)
,summary_harvests as 
(
    select 
        cycle_uuid,
        date_key,
        status,
        emergency_reasons,
        emergency_causes,
        COALESCE(is_early_harvest, FALSE) AS is_early_harvest,
        COUNT(DISTINCT CASE WHEN is_total = TRUE THEN is_total END) AS is_total,
        -- Aggregations
        AVG(size) AS commodity_per_kg,
        SUM(weight) AS harvest_weight,
        SUM(weight_fresh_1) AS harvest_weight_fresh_1,
        SUM(weight_fresh_2) AS harvest_weight_fresh_2,
        SUM(weight_fresh_3) AS harvest_weight_fresh_3,
        SUM(weight_bs) AS harvest_weight_bs,
        SUM(weight_km) AS harvest_weight_km,
        SUM(size_fresh_1) AS harvest_size_fresh_1,
        SUM(size_fresh_2) AS harvest_size_fresh_2,
        SUM(size_fresh_3) AS harvest_size_fresh_3,
        SUM(size_bs) AS harvest_size_bs,
        SUM(size_km) AS harvest_size_km,
        AVG(price_per_kg) AS price_per_kg,
        COUNT(DISTINCT uuid) AS total_input_harvest
     from
    (   
        SELECT
            harvest.cycle_uuid,
            harvest.uuid,
            harvest.action_date AS date_key,
            CASE 
                WHEN harvest.uuid IN ('0f060756-169c-4ec3-83e9-4a15e6e07151','9c938a2c-a470-4276-8d69-e88e36a39eb9') 
                THEN 'NORMAL' 
                ELSE status 
            END AS status,
            harvest.is_early_harvest,
            harvest.is_total,
            harvest_details.size size,
            harvest_details.weight weight,
            harvest_details.weight_fresh_1,
            harvest_details.weight_fresh_2,
            harvest_details.weight_fresh_3,
            harvest_details.weight_bs,
            harvest_details.weight_km,
            harvest_details.size_fresh_1,
            harvest_details.size_fresh_2,
            harvest_details.size_fresh_3,
            harvest.emergency_reasons,
            harvest.emergency_causes,
            harvest_details.size_bs,
            harvest_details.size_km,
            harvest_details.price_per_kg AS price_per_kg,
            ROW_NUMBER() OVER (PARTITION BY harvest.cycle_uuid, action_date ORDER BY harvest.created_at DESC) AS rn
    from 
        {{ source('staging','stg_cultivation_harvests') }} harvest
    left join
        (
        SELECT 
            harvest_uuid, 
            AVG(CAST(size AS NUMERIC)) AS size, 
            SUM(CAST(weight AS NUMERIC)) AS weight, 
            AVG(CAST(price_per_kg AS NUMERIC)) AS price_per_kg,
            COALESCE(SUM(CASE WHEN type = 'FRESH' AND rn = 1 THEN CAST(weight AS NUMERIC) END), 0) AS weight_fresh_1,
            COALESCE(SUM(CASE WHEN type = 'FRESH' AND rn = 2 THEN CAST(weight AS NUMERIC) END), 0) AS weight_fresh_2,
            COALESCE(SUM(CASE WHEN type = 'FRESH' AND rn = 3 THEN CAST(weight AS NUMERIC) END), 0) AS weight_fresh_3,
            SUM(CASE WHEN type = 'BS' THEN CAST(weight AS NUMERIC) END) AS weight_bs,
            SUM(CASE WHEN type = 'KM' THEN CAST(weight AS NUMERIC) END) AS weight_km,
            COALESCE(SUM(CASE WHEN type = 'FRESH' AND rn = 1 THEN CAST(size AS NUMERIC) END), 0) AS size_fresh_1,
            COALESCE(SUM(CASE WHEN type = 'FRESH' AND rn = 2 THEN CAST(size AS NUMERIC) END), 0) AS size_fresh_2,
            COALESCE(SUM(CASE WHEN type = 'FRESH' AND rn = 3 THEN CAST(size AS NUMERIC) END), 0) AS size_fresh_3,
            SUM(CASE WHEN type = 'BS' THEN CAST(size AS NUMERIC) END) AS size_bs,
            SUM(CASE WHEN type = 'KM' THEN CAST(size AS NUMERIC) END) AS size_km
        FROM 
            (select uuid, harvest_uuid, type, size, weight, price_per_kg
            ,COUNT(CASE WHEN type = 'FRESH' THEN harvest_uuid END) OVER (PARTITION BY harvest_uuid) AS total_harvest_uuid_fresh
            ,CASE 
                WHEN type = 'FRESH' THEN ROW_NUMBER() OVER (PARTITION BY harvest_uuid, type ORDER BY uuid)
                ELSE NULL 
            END AS rn 
            from {{ source('staging','stg_cultivation_harvest_details') }} 
            where created_by
            not in (
            'bavian.adi@efishery.com'
            ,'maq.riki@efishery.com'
            ,'rangga.luthanza@efishery.com'
            ,'tubagus.assad@efishery.com'
            ,'ade.yudha@efishery.com'
            ,'althof.ramdhan@efishery.com'
            ,'arief.yusron@efishery.com'
            ,'muhammad.aditya@efishery.com'
            ,'doni.pebruwantoro@efishery.com'
            ,'wihlarko.prasdegho@efishery.com'
            ,'andriannus.parasian@efishery.com'
            ,'rachmat.adi@efishery.com'
            ,'riko.pernando@efishery.com'
            ,'roy.parsaoran@efishery.com'
            ,'m.sholeh@efishery.com'
            ,'elsa.vinietta@efishery.com')
            ) a 
        group by 1) harvest_details 
        on harvest.uuid = harvest_details.harvest_uuid
    where 
        harvest.deleted_at is null and harvest.cycle_uuid is not null
        and harvest.uuid not in (
        'dc664d65-1329-47f8-9d3c-4058706af885'
        ,'b36f9732-7581-441c-8b37-c3df2e928055'
        ,'5a8ff671-2a32-4870-b931-2bc1214e7aff'
        ,'7b62da66-d4e5-4377-9321-0b8832a4780b'
        ,'fdbb9831-ea3e-4a74-8fc6-3f2b190da91f'
        ,'df5baf77-1336-4f07-9014-3ca97f433a9e'
        ,'cb28b301-9aa8-45a7-b993-8619dbb43ed5'
        ,'db66d184-94b1-43ad-bd0c-3adbcf8bcbe5'
        ) -- manually deleted
) t 
    -- where rn=1
    group by 1,2,3,4,5,6
)

,summary_diseases as (
    select 
       a.cycle_uuid
        ,a.action_date date_key
        ,b.physical_symptoms 
        ,sum(cast(b.dead_commodities as numeric)) dead_commodities 
        ,sum(cast(b.dead_commodities_weight as numeric)) dead_commodities_weight 
        ,avg(cast(b.mbw as numeric)) mbw 
        ,count(distinct case when b.is_disposed=true then b.is_disposed end) is_disposed 
        ,string_agg(distinct case when b.disease is not null then b.disease end, ',') disease 
        -- ,string_agg(distinct cast(case when physical_symptoms is not null then physical_symptoms end as string), ',') physical_symptoms 
    from 
        {{ source('staging','stg_cultivation_diseases') }} a
    left join
        {{ source('staging','stg_cultivation_disease_details') }} b on a.uuid = b.disease_uuid
    where 
        a.deleted_at is null and a.cycle_uuid is not null
        and a.created_by
        not in (
        'bavian.adi@efishery.com'
        ,'maq.riki@efishery.com'
        ,'rangga.luthanza@efishery.com'
        ,'tubagus.assad@efishery.com'
        ,'ade.yudha@efishery.com'
        ,'althof.ramdhan@efishery.com'
        ,'arief.yusron@efishery.com'
        ,'muhammad.aditya@efishery.com'
        ,'doni.pebruwantoro@efishery.com'
        ,'wihlarko.prasdegho@efishery.com'
        ,'andriannus.parasian@efishery.com'
        ,'rachmat.adi@efishery.com'
        ,'riko.pernando@efishery.com'
        ,'roy.parsaoran@efishery.com'
        ,'m.sholeh@efishery.com'
        ,'elsa.vinietta@efishery.com')
        and 
        b.deleted_at is null
    group by 1,2,3
)
,summary_bacterias as
(
    select 
        cycle_uuid,action_date date_key
        ,sum(cast(bacteria as numeric)) bacteria
        ,sum(cast(vibrio as numeric)) vibrio
        ,sum(cast(vibrio_yellow as numeric)) vibrio_yellow
        ,sum(cast(vibrio_green as numeric)) vibrio_green
        ,sum(cast(vibrio_luminance as numeric)) vibrio_luminance
        ,sum(cast(vibrio_black as numeric)) vibrio_black
    from 
        {{ source('staging','stg_cultivation_bacterias') }}
    where 
        deleted_at is null and cycle_uuid is not null
        and created_by
        not in (
        'bavian.adi@efishery.com'
        ,'maq.riki@efishery.com'
        ,'rangga.luthanza@efishery.com'
        ,'tubagus.assad@efishery.com'
        ,'ade.yudha@efishery.com'
        ,'althof.ramdhan@efishery.com'
        ,'arief.yusron@efishery.com'
        ,'muhammad.aditya@efishery.com'
        ,'doni.pebruwantoro@efishery.com'
        ,'wihlarko.prasdegho@efishery.com'
        ,'andriannus.parasian@efishery.com'
        ,'rachmat.adi@efishery.com'
        ,'riko.pernando@efishery.com'
        ,'roy.parsaoran@efishery.com'
        ,'m.sholeh@efishery.com'
        ,'elsa.vinietta@efishery.com')
    group by 1,2
)
,summary_hepato as (
    select
       cycle_uuid
       ,action_date date_key
       ,string_agg(distinct case when lipid_condition is not null and lipid_condition!='' then lower(lipid_condition) end, ', ') lipid_condition
       ,string_agg(distinct case when tubules_condition is not null and tubules_condition!='' then lower(tubules_condition) end, ', ') tubules_condition
       ,string_agg(distinct case when necrosis is not null and necrosis!='' then lower(necrosis) end, ', ') is_necrosis
       ,string_agg(distinct case when ehp is not null and ehp!='' then lower(ehp) end, ', ') is_ehp
    from 
        {{ source('staging','stg_cultivation_hepatopancreas') }}
    where 
        deleted_at is null and cycle_uuid is not null
        and created_by
        not in (
        'bavian.adi@efishery.com'
        ,'maq.riki@efishery.com'
        ,'rangga.luthanza@efishery.com'
        ,'tubagus.assad@efishery.com'
        ,'ade.yudha@efishery.com'
        ,'althof.ramdhan@efishery.com'
        ,'arief.yusron@efishery.com'
        ,'muhammad.aditya@efishery.com'
        ,'doni.pebruwantoro@efishery.com'
        ,'wihlarko.prasdegho@efishery.com'
        ,'andriannus.parasian@efishery.com'
        ,'rachmat.adi@efishery.com'
        ,'riko.pernando@efishery.com'
        ,'roy.parsaoran@efishery.com'
        ,'m.sholeh@efishery.com'
        ,'elsa.vinietta@efishery.com')
    group by 1,2
)
,summary_intestines as (
    select
       cycle_uuid
       ,action_date date_key
       ,string_agg(distinct case when climatophera is not null and climatophera!='' then lower(climatophera) end, ', ') is_climatophera
       ,string_agg(distinct case when intestines_tissue_color is not null and intestines_tissue_color!='' then lower(intestines_tissue_color) end, ', ') intestines_tissue_color
    from 
        {{ source('staging','stg_cultivation_intestines') }}
    where 
        deleted_at is null and cycle_uuid is not null
        and created_by
        not in (
        'bavian.adi@efishery.com'
        ,'maq.riki@efishery.com'
        ,'rangga.luthanza@efishery.com'
        ,'tubagus.assad@efishery.com'
        ,'ade.yudha@efishery.com'
        ,'althof.ramdhan@efishery.com'
        ,'arief.yusron@efishery.com'
        ,'muhammad.aditya@efishery.com'
        ,'doni.pebruwantoro@efishery.com'
        ,'wihlarko.prasdegho@efishery.com'
        ,'andriannus.parasian@efishery.com'
        ,'rachmat.adi@efishery.com'
        ,'riko.pernando@efishery.com'
        ,'roy.parsaoran@efishery.com'
        ,'m.sholeh@efishery.com'
        ,'elsa.vinietta@efishery.com')
    group by 1,2
)
,summary_all as (
    SELECT DISTINCT 
        a.*,
        i.pond_uuid,
        l.farm_uuid,
        k.feed_brand AS feed_brand_farm_surveys,
        COALESCE(j.leads_id, j.real_leads_id) AS lead_id,
        j.name AS pond_name,
        fish_type,
        pond_type,
        pond_shape,
        j.large AS large_pond,
        i.commodity_type,
        i.start_date AS start_cultivation,
        i.commodity_count_init,
        i.commodity_weight_init,
        CAST((i.commodity_count_init * i.commodity_weight_init) AS NUMERIC) AS total_commodity_weight,
        CASE 
            WHEN CAST(j.large AS NUMERIC) > 0 THEN CAST(i.commodity_count_init AS NUMERIC) / CAST(j.large AS NUMERIC)
            ELSE NULL 
        END AS spread_densities,
        c.feeding_type_category,
        CAST(c.feed_size AS NUMERIC) AS feed_size,
        c.feed_weight,
        c.feeding_type_value,
        c.cumulative_feed_weight,
        c.total_pakan,
        c.feed_name_combination,
        d.mbw AS mbw_samplings,
        d.shrimp_length AS length_samplings,
        d.fr / 100 AS fr_samplings,
        d.is_healthy,
        d.commodity_gills,
        d.commodity_swim_legs,
        d.commodity_intestine_colour,
        d.commodity_tail,
        d.commodity_hepato,
        d.commodity_colour,
        d.commodity_shell,
        d.commodity_flesh,
        d.is_diseased,
        d.disease AS disease_samplings,
        ratio_green_algae,
        ratio_blue_green_algae,
        ratio_golden_green_algae,
        ratio_diatom,
        ratio_euglenophyta,
        ratio_dinoflagelata,
        ratio_protozoa_zooplankton,
        CAST(total_plankton AS NUMERIC) AS total_plankton,
        f.is_total,
        f.emergency_reasons,
        f.emergency_causes,
        f.commodity_per_kg,
        f.harvest_weight,
        f.harvest_weight_fresh_1,
        f.harvest_weight_fresh_2,
        f.harvest_weight_fresh_3,
        f.harvest_weight_bs,
        f.harvest_weight_km,
        f.harvest_size_km,
        f.harvest_size_fresh_1,
        f.harvest_size_fresh_2,
        f.harvest_size_fresh_3,
        f.harvest_size_bs,
        f.status AS status_harvest,
        f.is_early_harvest,
        CAST(f.price_per_kg AS NUMERIC) AS price_per_kg,
        CAST(g.dead_commodities AS NUMERIC) AS dead_commodities,
        g.dead_commodities_weight,
        g.mbw AS mbw_diseases,
        g.is_disposed,
        g.disease,
        g.physical_symptoms,
        h.bacteria,
        h.vibrio,
        h.vibrio_yellow,
        h.vibrio_green,
        h.vibrio_luminance,
        h.vibrio_black,
        weather,
        water_colour,
        tan_all,
        tom_all,
        tss_all,
        ci_all,
        orp_all,
        no2_all,
        no3_all,
        water_hardness_all,
        nh3_all,
        nh4_all,
        fe_all,
        po4_all,
        ca_all,
        mg_all,
        co3_all,
        hco3_all,
        alkalinity_total_all,
        brightness_all,
        cod_all,
        temperature_all,
        ph_all,
        value_do_all,
        kincir_all,
        water_height_all,
        tds_all,
        water_clarity_all,
        salinity,
        weather_pagi,
        water_colour_pagi,
        tan_pagi,
        tom_pagi,
        tss_pagi,
        ci_pagi,
        orp_pagi,
        no2_pagi,
        no3_pagi,
        water_hardness_pagi,
        nh3_pagi,
        nh4_pagi,
        fe_pagi,
        po4_pagi,
        ca_pagi,
        mg_pagi,
        co3_pagi,
        hco3_pagi,
        alkalinity_total_pagi,
        brightness_pagi,
        cod_pagi,
        temperature_pagi,
        ph_pagi,
        value_do_pagi,
        kincir_pagi,
        water_height_pagi,
        tds_pagi,
        water_clarity_pagi,
        salinity_pagi,
        weather_sore,
        water_colour_sore,
        tan_sore,
        tom_sore,
        tss_sore,
        ci_sore,
        orp_sore,
        no2_sore,
        no3_sore,
        water_hardness_sore,
        nh3_sore,
        nh4_sore,
        fe_sore,
        po4_sore,
        ca_sore,
        mg_sore,
        co3_sore,
        hco3_sore,
        alkalinity_total_sore,
        brightness_sore,
        cod_sore,
        temperature_sore,
        ph_sore,
        value_do_sore,
        kincir_sore,
        water_height_sore,
        tds_sore,
        water_clarity_sore,
        salinity_sore,
        weather_malam,
        water_colour_malam,
        tan_malam,
        tom_malam,
        tss_malam,
        ci_malam,
        orp_malam,
        no2_malam,
        no3_malam,
        water_hardness_malam,
        nh3_malam,
        nh4_malam,
        fe_malam,
        po4_malam,
        ca_malam,
        mg_malam,
        co3_malam,
        hco3_malam,
        alkalinity_total_malam,
        brightness_malam,
        cod_malam,
        temperature_malam,
        ph_malam,
        value_do_malam,
        kincir_malam,
        water_height_malam,
        tds_malam,
        water_clarity_malam,
        salinity_malam,
        is_siphon,
        is_over_probiotik,
        is_over_feed,
        is_water_circulated,
        season,
        is_climatophera,
        intestines_tissue_color,
        lipid_condition,
        tubules_condition,
        is_necrosis,
        is_ehp,
        cast(DATE_DIFF(a.date_key, i.start_date, DAY) as int) as doc_,
        CASE 
            WHEN LOWER(feeding_type_category) LIKE '%fr%' AND feeding_type_value > 0 
            THEN (d.mbw * commodity_count_init * feeding_type_value) / 1000
            WHEN LOWER(feeding_type_category) LIKE '%index%' AND feeding_type_value > 0 
            AND CAST(DATE_DIFF(a.date_key, i.start_date, DAY) AS FLOAT64) >= 0 
            THEN (feeding_type_value * CAST(DATE_DIFF(a.date_key, i.start_date, DAY) AS FLOAT64) * CAST(commodity_count_init AS FLOAT64)) / 1000
        END AS total_pakan_daily,
        CASE 
            WHEN f.commodity_per_kg IS NULL OR f.commodity_per_kg = 0 THEN 0
            WHEN COALESCE(1000 / f.commodity_per_kg, 0) <= 100 THEN COALESCE(1000 / f.commodity_per_kg, 0)
            ELSE 0
        END AS abw_harvest,
        ROUND(COALESCE(f.harvest_weight * f.commodity_per_kg, 0)) AS population_harvest,
        CASE 
            WHEN d.mbw > 0 THEN 1000 / d.mbw
        END AS size_samplings,
        CASE 
            WHEN d.mbw > 0 AND DATE_DIFF(a.date_key,  i.start_date,DAY) > 0 THEN d.mbw / CAST(DATE_DIFF(a.date_key, i.start_date,DAY) AS NUMERIC)
        END AS adg_cumulative_samplings,
        CASE 
            WHEN (d.fr / 100 > 0 OR (LOWER(feeding_type_category) LIKE '%fr%' AND feeding_type_value > 0)) AND COALESCE(c.total_pakan, c.feed_weight) > 0 THEN (CASE 
                WHEN c.total_pakan > 0 THEN c.total_pakan
                WHEN c.feed_weight > 0 THEN c.feed_weight
            END) / (CASE 
                WHEN feeding_type_value > 0 THEN feeding_type_value
                WHEN d.fr > 0 THEN d.fr
                ELSE 11.9 * (POWER(d.mbw, -0.528)) / 100
            END)
        END AS biomass_samplings
    FROM base_action_cultivation a
    left join summary_waters b
        on a.cycle_uuid=b.cycle_uuid and a.date_key=b.date_key
    left join summary_feedings c
        on a.cycle_uuid=c.cycle_uuid and a.date_key=c.date_key
    left join summary_samplings d
        on a.cycle_uuid=d.cycle_uuid and a.date_key=d.date_key
    left join summary_planktons e
        on a.cycle_uuid=e.cycle_uuid and a.date_key=e.date_key
    left join summary_harvests f
        on a.cycle_uuid=f.cycle_uuid and a.date_key=f.date_key
    left join summary_diseases g
        on a.cycle_uuid=g.cycle_uuid and a.date_key=g.date_key
    left join summary_bacterias h
        on a.cycle_uuid=h.cycle_uuid and a.date_key=h.date_key
    left join summary_water_conditioning b1
        on a.cycle_uuid=b1.cycle_uuid and a.date_key=b1.date_key
    left join summary_hepato m
        on a.cycle_uuid=m.cycle_uuid and a.date_key=m.date_key
    left join summary_intestines n
        on a.cycle_uuid=n.cycle_uuid and a.date_key=n.date_key
    left join {{ source('staging','stg_cultivation_cycles') }} i
        on a.cycle_uuid=i.uuid
    left join {{ source('staging','stg_cultivation_ponds') }} j
        on i.pond_uuid=j.uuid
    left join {{ source('staging','stg_cultivation_farm_blocks') }} l
        on l.uuid =j.block_uuid
    left join (select distinct farm_uuid, STRING_AGG(feed_brand, ', ') as feed_brand from {{ source('staging','stg_cultivation_farm_surveys') }} group by farm_uuid) k
        on k.farm_uuid=l.farm_uuid
    where j.migration_source in 
    ('TOOLS_BUDIDAYA_V1'
    ,'KOLAMKU_SHRIMP'
    ,'BACKFILL_TOOLS_BUDIDAYA_V1'
    ,'APP_SOURCE_CULTIVATION'
    ,'TOOLS_BUDIDAYA_V1_SHRIMP'
    ,'BACKFILL_APP_SOURCE_CULTIVATION')
    or
    (lower(commodity_type) like '%udang%' or lower(commodity_type) like '%shrimp%' or 
    commodity_type='' or commodity_type is null)
    or j.appsource in ('APP_SOURCE_CULTIVATION','TOOLS_BUDIDAYA_V1')
    and i.created_by
    not in (
    'bavian.adi@efishery.com'
    ,'maq.riki@efishery.com'
    ,'rangga.luthanza@efishery.com'
    ,'tubagus.assad@efishery.com'
    ,'ade.yudha@efishery.com'
    ,'althof.ramdhan@efishery.com'
    ,'arief.yusron@efishery.com'
    ,'muhammad.aditya@efishery.com'
    ,'doni.pebruwantoro@efishery.com'
    ,'wihlarko.prasdegho@efishery.com'
    ,'andriannus.parasian@efishery.com'
    ,'rachmat.adi@efishery.com'
    ,'riko.pernando@efishery.com'
    ,'roy.parsaoran@efishery.com'
    ,'m.sholeh@efishery.com'
    ,'elsa.vinietta@efishery.com')
)

,t_reference_product_cpp as (
select cast(doc as integer) as doc,cast(mbw as numeric) as mbw ,cast(sr as numeric) as sr
from {{ source('staging','stg_gsheet_reference_pakan') }}
where brand='CPP'
)

,tref_sr as (
select doc,sr
from t_reference_product_cpp
union all
select 
    a.*
    ,case when doc_ref=1 then 100 else 1.0*(100-((doc_ref-29)*laju_kematian)) end
from
(
select (a.sr-b.sr)/(b.doc-a.doc) laju_kematian
from 
(select doc,sr from t_reference_product_cpp where doc=29) a
cross join
(select doc,sr from t_reference_product_cpp where doc=120) b
) b
cross join (SELECT 
    number AS doc_ref
FROM 
    UNNEST(GENERATE_ARRAY(1, 200)) AS number) a
where doc_ref>120
)

,rpt_all as 
(
select 
    distinct 
    to_hex(md5(concat(coalesce(cycle_uuid,'UNKNOWN'),'_',coalesce(CAST(date_key AS STRING),'UNKNOWN')))) as efarm_cultivation_aquaculture_id
    ,cycle_uuid
    ,contributors
    ,data_collection_method
    ,appsource
    ,date_key as period_date
    ,pond_uuid
    ,farm_uuid
    ,feed_brand_farm_surveys
    ,lead_id
    ,pond_name
    ,fish_type
    ,pond_type
    ,pond_shape
    ,cast(large_pond as numeric) as large_pond
    ,commodity_type
    ,start_cultivation as start_cultivation_date
    ,commodity_count_init
    ,cast(commodity_weight_init as numeric) as commodity_weight_init
    ,total_commodity_weight
    ,spread_densities
    ,feeding_type_category
    ,cast(feed_size as numeric) as feed_size
    ,feed_weight
    ,feeding_type_value
    ,cumulative_feed_weight
    ,total_pakan
    ,feed_name_combination
    ,mbw_samplings
    ,length_samplings
    ,fr_samplings
    ,CAST(is_healthy as boolean) is_healthy
    ,commodity_gills
    ,commodity_swim_legs
    ,commodity_intestine_colour
    ,commodity_tail
    ,commodity_hepato
    ,commodity_colour
    ,commodity_shell
    ,commodity_flesh
    ,CAST(is_diseased as boolean) is_diseased
    ,disease_samplings
    ,ratio_green_algae
    ,ratio_blue_green_algae
    ,ratio_golden_green_algae
    ,ratio_diatom
    ,ratio_euglenophyta
    ,ratio_dinoflagelata
    ,ratio_protozoa_zooplankton
    ,total_plankton
    ,case when is_total = 1 then true when is_total = 0 then false else null end as is_total_harvest
    ,emergency_reasons
    ,emergency_causes
    ,commodity_per_kg
    ,harvest_weight
    ,harvest_weight_fresh_1
    ,harvest_weight_fresh_2
    ,harvest_weight_fresh_3
    -- ,harvest_weight_fresh
    ,harvest_weight_bs
    ,harvest_weight_km
    ,harvest_size_km
    ,harvest_size_fresh_1
    ,harvest_size_fresh_2
    ,harvest_size_fresh_3
    -- ,harvest_size_fresh
    ,harvest_size_bs
    ,status_harvest
    ,is_early_harvest
    ,price_per_kg
    ,cast(dead_commodities as numeric) as dead_commodities
    ,dead_commodities_weight
    ,mbw_diseases
    ,case when is_disposed = 1 then true when is_disposed = 0 then false else null end as is_disposed 
    ,disease
    ,physical_symptoms
    ,bacteria
    ,vibrio
    ,vibrio_yellow
    ,vibrio_green
    ,vibrio_luminance
    ,vibrio_black
    ,weather
    ,water_colour
    ,tan_all
    ,tom_all
    ,tss_all
    ,ci_all
    ,orp_all
    ,no2_all
    ,no3_all
    ,water_hardness_all
    ,nh3_all
    ,nh4_all
    ,fe_all
    ,po4_all
    ,ca_all
    ,mg_all
    ,co3_all
    ,hco3_all
    ,alkalinity_total_all
    ,brightness_all
    ,cod_all
    ,temperature_all
    ,ph_all
    ,value_do_all
    ,kincir_all
    ,water_height_all
    ,tds_all
    ,water_clarity_all
    ,salinity
    ,weather_pagi
    ,water_colour_pagi
    ,tan_pagi
    ,tom_pagi
    ,tss_pagi
    ,ci_pagi
    ,orp_pagi
    ,no2_pagi
    ,no3_pagi
    ,water_hardness_pagi
    ,nh3_pagi
    ,nh4_pagi
    ,fe_pagi
    ,po4_pagi
    ,ca_pagi
    ,mg_pagi
    ,co3_pagi
    ,hco3_pagi
    ,alkalinity_total_pagi
    ,brightness_pagi
    ,cod_pagi
    ,temperature_pagi
    ,ph_pagi
    ,value_do_pagi
    ,kincir_pagi
    ,water_height_pagi
    ,tds_pagi
    ,water_clarity_pagi
    ,salinity_pagi
    ,weather_sore
    ,water_colour_sore
    ,tan_sore
    ,tom_sore
    ,tss_sore
    ,ci_sore
    ,orp_sore
    ,no2_sore
    ,no3_sore
    ,water_hardness_sore
    ,nh3_sore
    ,nh4_sore
    ,fe_sore
    ,po4_sore
    ,ca_sore
    ,mg_sore
    ,co3_sore
    ,hco3_sore
    ,alkalinity_total_sore
    ,brightness_sore
    ,cod_sore
    ,temperature_sore
    ,ph_sore
    ,value_do_sore
    ,kincir_sore
    ,water_height_sore
    ,tds_sore
    ,water_clarity_sore
    ,salinity_sore
    ,weather_malam
    ,water_colour_malam
    ,tan_malam
    ,tom_malam
    ,tss_malam
    ,ci_malam
    ,orp_malam
    ,no2_malam
    ,no3_malam
    ,water_hardness_malam
    ,nh3_malam
    ,nh4_malam
    ,fe_malam
    ,po4_malam
    ,ca_malam
    ,mg_malam
    ,co3_malam
    ,hco3_malam
    ,alkalinity_total_malam
    ,brightness_malam
    ,cod_malam
    ,temperature_malam
    ,ph_malam
    ,value_do_malam
    ,kincir_malam
    ,water_height_malam
    ,tds_malam
    ,water_clarity_malam
    ,salinity_malam
    ,is_siphon
    ,is_over_probiotik
    ,is_over_feed
    ,is_water_circulated
    ,is_climatophera
    ,intestines_tissue_color
    ,lipid_condition
    ,tubules_condition
    ,is_necrosis
    ,is_ehp
    ,season
    ,doc_
    ,cast(total_pakan_daily as numeric) as total_pakan_daily
    ,abw_harvest
    ,cast(population_harvest as numeric) as population_harvest
    ,size_samplings
    ,adg_cumulative_samplings
    ,biomass_samplings 
    ,cast(coalesce(cumulative_feed_weight,total_pakan_daily*doc_)/biomass_samplings as numeric) as fcr
    ,case when mbw_samplings=0 then null
    when biomass_samplings/(mbw_samplings/1000)>commodity_count_init and commodity_count_init is null and commodity_count_init=0 then null else biomass_samplings/(mbw_samplings/1000) end as population_samplings
    ,case when mbw_samplings=0 then null else
    ((biomass_samplings/(mbw_samplings/1000))/commodity_count_init)*100 end as sr_samplings
    ,tref_sr.sr/100 ref_sr
from summary_all
left join tref_sr
on summary_all.doc_=tref_sr.doc
where 
    lead_id not in (select leads_id from filter_account_prod)
    and concat(cast(date_key as string),'_',cast(cycle_uuid as string)) not in ('2024-09-10_634735b5-a61e-4323-b6c6-ff3b77a47483','2024-09-18_634735b5-a61e-4323-b6c6-ff3b77a47483','2024-09-21_634735b5-a61e-4323-b6c6-ff3b77a47483','2024-12-03_9fbbd4d8-8f1f-4fde-bc67-2b6c278c0f7b','2029-05-17_66b39007-3dd5-4657-a238-87928bd24f98','2328-10-28_189c01fd-4855-5d3c-db5c-aa13f90d3989')
    and lead_id is not null
)

select * from rpt_all
