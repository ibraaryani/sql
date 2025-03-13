with end_pks as (
select 
    distinct
        lead_id
        , start_validity_period
        , end_validity_period
    ,case 
        when pks_inactive_since_at < end_validity_period then pks_inactive_since_at
        else end_validity_period
    end as real_end_validity_period
    ,pks_type
from
    {{ ref('dim_pks') }}
where 
        business_partner_group IN ('EMALL_FFMS','CULTURIST','EMALL_B2B')
        and business_partner_type IN ('PKS_COMMERCIAL','CULTURIST_SHRIMP','PKS_DAFNOM') 
        and source_app IN ('FEEDFUND_DB','GSHEET_PKS_SHRIMP','GSHEET_PKS_DAFNOM')
        -- and pks_number IS NOT NULL
UNION DISTINCT
select 
    distinct
        lead_id
        , start_validity_period
        , end_validity_period
    ,case 
        when pks_inactive_since_at < end_validity_period then pks_inactive_since_at
        else end_validity_period
    end as real_end_validity_period
    ,pks_type
from
    {{ ref('dim_pks') }}
where 
business_partner_group IN ('EMALL_CUSTOMER')
        and business_partner_type IN ('PKS_CUSTOMER') 
        and source_app IN ('FEEDFUND_DB')
        -- and pks_number IS NOT NULL
        and is_valid_customer is true
)

, new_cte_pks_historical_raw as 
(
    select distinct
        raw_data.lead_id AS farmer_lead_id
        , cast(start_validity_period as DATE) as pks_date
        , cast(real_end_validity_period as DATE) as pks_end_date
    from end_pks AS raw_data
    where 
       pks_type IN ('CONTRACT_FARMING','CONTRACT_FARMING_RENEWAL','CFSHRIMP')
        -- and pks_number IS NOT NULL
)
,base_cycle as (
select 
    *,row_number()over(partition by cycle_uuid order by case when resign_status='Active' then 1 else 2 end) rn 
from (select distinct *,input_sources_cycle_creator input_sources,status_cycle_creator resign_status
from 
{{ ref('fact_cultivation_active_cycle') }}) 
)
,tagging_ts_shwl as (
select 
    distinct
    case when input_sources in ('Area Manager','Field Business Development Officer') then 'BD'
    when lower(input_sources) like '%cultivation%' or lower(input_sources) like '%technical%' 
    or input_sources ='BI Analyst' or input_sources='Product & Project Management Analyst' then 'TS'
    when lower(input_sources) like '%limnology%' or cycle_created_by in (
    'aldi.iransyah@efishery.com'
    ,'berlian.rifandi@efishery.com'
    ,'rino.alisandri@efishery.com'
    ,'nur.islami@efishery.com'
    ,'muhammad.idham@efishery.com'
    ,'indri.ariani@efishery.com'
    ,'sihlvia.oktanita@efishery.com'
    ,'prahara.putri@efishery.com'
    ,'firda.nurdiana@efishery.com'
    ,'widodo.widyantoko@efishery.com'
    ,'yurmasari@efishery.com'
    ,'icut.eva@efishery.com'
    ,'siti.nadhiroh@efishery.com'
    ,'nevia.yukilla@efishery.com'
    ,'uky.wijayanti@efishery.com'
    ,'khalimatus.sadiyah@efishery.com'
    ,'yulia.musna@efishery.com'
    ,'wahyu.meganingrum@efishery.com'
    ,'muhklas.shahwinarno@efishery.com'
    ) then 'SHWL'
    else 'Others' end tag_input_cycle
    ,cycle_uuid
from base_cycle
where rn=1
)

, t_farmer_cf as 
(
select 
    distinct 
    farmer_lead_id
    ,b.date
from new_cte_pks_historical_raw a 
left join {{ ref('dim_date') }} b 
on b.date between a.pks_date and a.pks_end_date
)

/*
-- Harvest emergency dan normal harvest (priority normal)
-- Harvest total dan partial (priority total)
-- Traditional dan semi intensive (priority semi intensive) done
-- Tipe Farmers (CF vs Non CF) done
-- Date done
*/
-- start cte data productivity shrimp
,base 
as 
(
select 
distinct
  cycle_uuid
  ,contributors
  ,period_date
  ,pond_uuid
  ,pond_name
  ,cultivation_type
  ,pond_type
  ,case when c.farmer_lead_id is not null then 'CF' else 'Non CF' end subscription_cf
  ,lead_id 
  ,large_pond
  ,start_cultivation_date
  ,commodity_count_init
  ,total_commodity_weight
  ,mbw_samplings
  ,doc_
  ,commodity_type
  ,cumulative_feed_weight
  ,harvest_weight
  ,is_total_harvest
  ,status_harvest
  ,commodity_per_kg
  ,coalesce(harvest_weight_fresh_1,0) harvest_weight_fresh_1
  ,coalesce(harvest_size_fresh_1,0) harvest_size_fresh_1
  ,coalesce(harvest_weight_fresh_2,0) harvest_weight_fresh_2
  ,coalesce(harvest_size_fresh_2,0) harvest_size_fresh_2
  ,coalesce(harvest_weight_fresh_3,0) harvest_weight_fresh_3
  ,coalesce(harvest_size_fresh_3,0) harvest_size_fresh_3
  ,coalesce(harvest_weight_bs,0) harvest_weight_bs
  ,coalesce(harvest_size_bs,0) harvest_size_bs
  ,coalesce(harvest_weight_km,0) harvest_weight_km
  ,coalesce(harvest_size_km,0) harvest_size_km
from 
  {{ ref('metric_efarm_cultivation_aquaculture') }} a 
left join 
    (
    select
    cycle_uuid cycle_uuid_fcac, cultivation_type
    from
    {{ ref('fact_cultivation_active_cycle') }}
    where
    cultivation_type is not null  
    )b 
  on a.cycle_uuid = b.cycle_uuid_fcac
left join t_farmer_cf c 
  on a.lead_id=c.farmer_lead_id and a.period_date=c.date
where
  (mbw_samplings>0 or harvest_weight>0 or cumulative_feed_weight>0)
)
,t_size as 
(
select 
    cycle_uuid, pond_uuid,lead_id,doc_,mbw_samplings,
    case when 1000/mbw_samplings<50 and doc_<=60 then 1 else 0 end is_invalid
    ,row_number()over(partition by cycle_uuid order by doc_) rn
from base
where doc_>0 and mbw_samplings>0
)
,t_cfw as 
(
select 
    cycle_uuid,pond_uuid,lead_id,doc_,cumulative_feed_weight
from base
where doc_>0 and cumulative_feed_weight>0
)
,baseh as
(
select 
    cycle_uuid,pond_uuid,lead_id
    ,doc_,is_total_harvest,commodity_per_kg,harvest_weight
    ,case when is_total_harvest=true and doc_<60 and commodity_per_kg<50 then 1 else 0 end is_invalid
    ,harvest_weight_fresh_1
    ,harvest_size_fresh_1
    ,harvest_weight_fresh_2
    ,harvest_size_fresh_2
    ,harvest_weight_fresh_3
    ,harvest_size_fresh_3
    ,harvest_weight_bs
    ,harvest_size_bs
    ,harvest_weight_km
    ,harvest_size_km
from 
    base
where 
    (commodity_type='Udang Vaname' or commodity_type is null) 
    and doc_>0 and harvest_weight>0 
)
,has_total_harvest as (
select 
    cycle_uuid,min(doc_) doc_
from baseh 
where is_invalid=0 and is_total_harvest=true
group by 1
)

,t_harvest as 
(
select 
    cycle_uuid,max(doc_) doc_,0 is_total
from baseh 
where is_invalid=0
and cycle_uuid not in (select cycle_uuid from has_total_harvest)
group by 1
union all 
select *,1 is_total
from has_total_harvest
)
,t_doc_ as (
select 
doc_,NTILE(100) OVER (ORDER BY doc_) rn
from t_harvest
)
,t_summary as 
(
select 0.25 quantile,round(avg(doc_)) nilai 
from t_doc_
where rn=25
union all
select 0.5 quantile,round(avg(doc_)) nilai 
from t_doc_
where rn=50
union all
select 0.75 quantile,round(avg(doc_)) nilai 
from t_doc_
where rn=75
)
,t_outlier as 
(
select 
    b.nilai-(1.5*(a.nilai-b.nilai)) lcl,a.nilai+(3*(a.nilai-b.nilai)) ucl
from 
(select nilai
from t_summary
where quantile= 0.75) a 
cross join
(select nilai
from t_summary
where quantile= 0.25) b
)
,t_harvest_2 as
(
select 
    *
    ,case when doc_ between (select lcl from t_outlier) and  (select ucl from t_outlier) then 0 
    when doc_<(select lcl from t_outlier) then -1
    else 1 end is_anomaly
from t_harvest
)
/*
-- where (doc_< (select lcl from t_outlier) and doc_ >(select ucl from t_outlier)) and is_total=1
*/
,t_harvest_3 as 
(
select 
    a.*,b.doc_ last_doc_,b.is_anomaly
    ,case when is_invalid=0 and is_anomaly=1 and a.doc_=b.doc_ then 1
    when is_anomaly=-1 then 1
    when is_invalid=1 then 1 
    else 0 end is_invalid_com
from 
baseh a
left join 
t_harvest_2 b
on a.cycle_uuid=b.cycle_uuid and a.doc_<=b.doc_
)

,dim_area as 
(
select 
    distinct a.culturist_id,a.culturist_name, b.area_code,b.region_code
from {{ ref('dim_partner') }} a 
left join {{ ref('dim_point') }} b
on a.point_id=b.point_id and valid_flag=true
where culturist_id is not null
)

,t_final as (
select 
  distinct
  to_hex(md5(concat(coalesce(a.cycle_uuid,'') , coalesce(cast(a.period_date as string),'')))) as unique_id
  ,a.cycle_uuid
  ,a.start_cultivation_date
  ,a.pond_type
  ,a.pond_name
  ,a.period_date
  ,a.lead_id
  ,g.culturist_name
  ,h.pks_type 
  , i.financing_subscription_type financing_subscription_type_from_cultivation_tools
  ,a.pond_uuid
  ,coalesce(area_code,'UNKNOWN') area_code
  ,coalesce(region_code,'UNKNOWN') region_code
  ,coalesce(a.cultivation_type,'UNKNOWN') cultivation_type
  ,a.subscription_cf
  ,case when status_harvest='' then 'UNKNOWN' else coalesce(status_harvest,'UNKNOWN') end status_harvest
/*
--   ,b.cumulative_feed_weight/((sr/100)*commodity_count_init*(d.mbw_samplings/1000)) FCR_sampling
--   ,d.mbw_samplings
*/
--   ,e.mbw_harvest
  , CASE
        WHEN size_harvest = 0 THEN 0
        ELSE 1000/size_harvest
      END AS mbw_harvest
  ,c.ref_sr
  ,case when e.is_total_harvest=true then 'Total' when e.is_total_harvest=false then 'Partial' end is_total_harvest
  ,commodity_count_init
  ,total_commodity_weight
  ,a.large_pond
  ,commodity_count_init/a.large_pond spread_densities
  ,cumulative_population_harvest
  ,cumulative_harvest_weight
  ,b.cumulative_feed_weight
  ,(cumulative_harvest_weight/1000)/(a.large_pond/10000) productivity
  ,cumulative_population_harvest/commodity_count_init sr_harvest
  ,b.cumulative_feed_weight/cumulative_harvest_weight fcr_harvest
  ,case when e.is_total_harvest=true then cumulative_population_harvest/commodity_count_init end sr_harvest_total
  ,case when e.is_total_harvest=true then b.cumulative_feed_weight/cumulative_harvest_weight end fcr_harvest_total
  ,coalesce(f.tag_input_cycle,'UNKNOWN') tag_input_cycle
  ,row_number()over(partition by a.cycle_uuid,date(date_trunc(a.period_date,MONTH)) order by period_date desc) rn 
  ,row_number()over(partition by a.cycle_uuid order by period_date desc) last_rn
  ,CASE 
        WHEN e.is_total_harvest=true AND 
             ROW_NUMBER() OVER (
                 PARTITION BY a.cycle_uuid 
                 ORDER BY CASE WHEN e.is_total_harvest=true THEN period_date END DESC
             ) = 1 
        THEN 1 
        ELSE 0 
    END AS is_total_harvest_and_last_period
from base a
left join t_cfw b 
on a.doc_=b.doc_ and a.cycle_uuid=b.cycle_uuid
left join 
    (select
    cycle_uuid cycle_uuid_metric
    ,period_date period_date_metric
    ,doc_ doc_metric
    ,ref_sr
    from 
    {{ ref('metric_efarm_cultivation_aquaculture') }}
    ) c 
on a.cycle_uuid = c.cycle_uuid_metric and a.doc_ = c.doc_metric
/*
-- left join
-- (
-- select 
--     cycle_uuid
--     ,doc_
--     ,mbw_samplings
-- from 
--     t_size
-- where is_invalid=0
-- ) d
-- on a.doc_=d.doc_ and a.cycle_uuid=d.cycle_uuid
*/
left join 
(
select 
    cycle_uuid
    ,doc_
    ,is_total_harvest
    -- ,commodity_per_kg size_harvest
    ,case 
        when harvest_weight_fresh_1>harvest_weight_fresh_2 and harvest_weight_fresh_1>harvest_weight_fresh_3 then harvest_size_fresh_1
        when harvest_weight_fresh_2>harvest_weight_fresh_1 and harvest_weight_fresh_2>harvest_weight_fresh_3 then harvest_size_fresh_2
        when harvest_weight_fresh_3>harvest_weight_fresh_1 and harvest_weight_fresh_3>harvest_weight_fresh_2 then harvest_size_fresh_3
        else 0
    end size_harvest
    -- , CASE
    --     WHEN commodity_per_kg = 0 THEN 0
    --     ELSE 1000/commodity_per_kg
    --   END AS mbw_harvest
    ,harvest_weight
    ,sum(harvest_weight)over(partition by cycle_uuid order by doc_) cumulative_harvest_weight
    -- ,sum(harvest_weight* commodity_per_kg)over(partition by cycle_uuid order by doc_) cumulative_population_harvest
    ,sum((harvest_weight_fresh_1*harvest_size_fresh_1)+(harvest_weight_fresh_2*harvest_size_fresh_2)+(harvest_weight_fresh_3*harvest_size_fresh_3)+(harvest_weight_bs*harvest_size_bs)+(harvest_weight_km*harvest_size_km))over(partition by cycle_uuid order by doc_) cumulative_population_harvest
from t_harvest_3
) e
on a.doc_=e.doc_ and a.cycle_uuid=e.cycle_uuid
left join tagging_ts_shwl f
on a.cycle_uuid=f.cycle_uuid
left join dim_area g 
on a.lead_id=g.culturist_id
left join 
( SELECT 
    lead_id,
    date,
     STRING_AGG(DISTINCT pks_type, ', ') AS pks_type
FROM 
    (SELECT 
      a.lead_id,
      b.date,
      a.pks_type
  FROM 
      end_pks a 
  LEFT JOIN 
      {{ ref('dim_date') }} b 
  ON 
      b.date BETWEEN a.start_validity_period AND a.real_end_validity_period) z
GROUP BY 
    lead_id, date
ORDER BY 
    date 
) h
on a.lead_id = h.lead_id and a.period_date = h.date
left join 
(select 
  distinct cycle_uuid
  , financing_subscription_type
from 
  {{ ref('fact_cultivation_active_cycle') }}) i
on a.cycle_uuid = i.cycle_uuid
where 
    e.is_total_harvest is not null
)
-- end cte data productivity shrimp

-- start cte data productivity fish
, t_metrics_efish as (
  select cycle_uuid
  , pond_type
  , spread_densities
  -- , mbw_harvest
  , biomass_harvest
  , sum(biomass_harvest) over(partition by cycle_uuid) cumulative_harvest_weight
  , sum(size_harvest * biomass_harvest) over(partition by cycle_uuid) cumulative_population_harvest
  , doc_
  , ref_sr
  , commodity_weight_init
  , total_commodity_weight
  , sum(total_feed) over(partition by cycle_uuid) cumulative_feed_weight
  , CASE
        WHEN size_harvest = 0 THEN 0
        ELSE 1000/size_harvest
      END AS mbw_harvest
  , period_date
  , case when is_total_harvest = true then period_date end harvest_total_date 
  -- , size_harvest
  -- , biomass_harvest
  -- , size_harvest * biomass_harvest
from {{ ref('metric_efish_cultivation_aquaculture')}}
where cycle_uuid in (select cycle_uuid from {{ ref('metric_efish_cultivation_aquaculture')}} where is_total_harvest = true)
order by cycle_uuid, doc_
)

, t_base_fish as (
  select 
  row_number() over(partition by aa.cycle_uuid order by doc_ desc) row_num_cycle
  , aa.*
  
  , bb.period_date
  , bb.pond_type
  , bb.harvest_total_date
  , bb.spread_densities
  , bb.cumulative_feed_weight
  , bb.cumulative_harvest_weight
  , bb.cumulative_population_harvest
  , bb.biomass_harvest
  , bb.ref_sr
  , bb.commodity_weight_init
  , bb.total_commodity_weight
  , bb.mbw_harvest
  -- , cc.pond_type
  -- , cc.lead_id
  from base_cycle aa
  left join t_metrics_efish bb 
  on aa.cycle_uuid = bb.cycle_uuid
  left join {{ ref('dim_pond') }} cc on aa.pond_uuid = cc.pond_id
  where bb.cycle_uuid is not null
  )
-- end cte data productivity fish

-- menggabungkan data fish & shrimp
select 
a.cycle_uuid
, start_cultivation start_cultivation_date
, harvest_total_date
, pond_uuid
, pond_name
, a.lead_id
, b.culturist_name farmer_name
, cultivation_type
, a.commodity_type
, pond_type
, large_pond
, commodity_count_init
, total_commodity_weight total_commodity_weight_init
, spread_densities
, cumulative_feed_weight
, cumulative_harvest_weight
, cumulative_population_harvest
, (cumulative_harvest_weight/1000)/(a.large_pond/10000) productivity
, cumulative_population_harvest/commodity_count_init sr_harvest
, ref_sr
, cumulative_feed_weight / NULLIF((cumulative_harvest_weight - (total_commodity_weight / 1000)), 0) AS fcr_harvest
, mbw_harvest
, 'UNKNOWN' status_harvest
, case when c.farmer_lead_id is not null then 'CF' else 'Non CF' end subscription_cf
, pks_type kabayan_type
, coalesce(f.tag_input_cycle,'UNKNOWN') tag_input_cycle
, area_code
, coalesce(region_code,'UNKNOWN') region_code
, FALSE as is_use_proprietary_solution 
, 'Fish' value_chain
from t_base_fish a
left join dim_area b on a.lead_id = b.culturist_id
left join t_farmer_cf c 
  on a.lead_id=c.farmer_lead_id and a.period_date=c.date
left join     ( SELECT 
        lead_id,
        date,
        STRING_AGG(DISTINCT pks_type, ', ') AS pks_type
    FROM 
        (SELECT 
        a.lead_id,
        b.date,
        a.pks_type
    FROM 
        end_pks a 
    LEFT JOIN 
        {{ ref('dim_date') }} b 
    ON 
        b.date BETWEEN a.start_validity_period AND a.real_end_validity_period) z
    GROUP BY 
        lead_id, date
    ORDER BY 
        date 
    ) h
  on a.lead_id = h.lead_id and a.period_date = h.date    
left join tagging_ts_shwl f on a.cycle_uuid=f.cycle_uuid


where row_num_cycle = 1

union all

select 
  aa.cycle_uuid
  , aa.start_cultivation_date
  , aa.period_date harvest_total_date
--   , is_total_harvest harvest_type
  , aa.pond_uuid
  , aa.pond_name
  , aa.lead_id
  , aa.culturist_name farmer_name
  , aa.cultivation_type
  , fcac.commodity_type
  , aa.pond_type
  , aa.large_pond
  , aa.commodity_count_init
  , aa.total_commodity_weight total_commodity_weight_init
  , aa.spread_densities
  , aa.cumulative_feed_weight
  , aa.cumulative_harvest_weight
  , aa.cumulative_population_harvest
  , aa.productivity
  , aa.sr_harvest
  , aa.ref_sr
  , fcr_harvest
  , aa.mbw_harvest
  , aa.status_harvest
  , aa.subscription_cf
  , aa.pks_type kabayan_type
  , aa.tag_input_cycle
  , aa.area_code
  , aa.region_code
  , FALSE as is_use_proprietary_solution 
  , "Shrimp" value_chain
from
  t_final aa
left join {{ ref('fact_cultivation_active_cycle') }} fcac on aa.cycle_uuid = fcac.cycle_uuid
where 
  is_total_harvest_and_last_period = 1
 order by 3 desc
