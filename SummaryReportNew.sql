with main1 as (select distinct
address as address,
name as name_,
district,
typeemkost as typeemkost,
pr_name as pr_name,
sum (rc) as emkostzakaz,
sum(rv) as zakazvolume,
sum(ersc) as vivoz,
sum(tiv) as vivozvolume,
sum(ervv) as factvolume,			   
DENSE_RANK() OVER ( order by  address)  AS num, DENSE_TEST
			   from
(select distinct s.address as address,
se.name,
l.name as district,	
ct.name as typeemkost,
pr.short_name as pr_name, sum(ri.count) rc, sum(round(ri.volume::numeric,2)) rv,
sum(tis.tic) tic, sum(tis.tiv) tiv,
sum(er.count) ersc, sum(round(er.volume::numeric,2)) ersv,  sum(round(tis.tirvv,2)) ervv


from request_item ri
inner join request re on re.request_id = ri.request_id 
inner join request_status rts on rts.request_status_id = re.request_status_id
and rts.type in ('Aproved', 'New') and re.planned_date between :RDI_begin and :RDI_end
left join x_task_item_execution_result er on er.request_id = re.request_id
left join (select distinct request_ref, sum(fact_count) tic, max(round(fact_accounting_volume::numeric,4)) tiv, max(round(fact_volume::numeric,4)) tirvv
from x_task_item ti inner join x_task_item_execution_result as tier on tier.task_item_execution_result_id = ti.execution_result_id
inner join x_task_item_container_result as tiers on tier.task_item_execution_result_id = tiers.task_item_execution_result_id
		   group by request_ref) tis on tis.request_ref = re.request_id
left join x_task_item_execution_result_status ers on ers.task_item_execution_result_status_id = er.status_id
inner join stand s on re.stand_id = s.stand_id
left join location l on l.location_id = s.address_district_id 
left join  sector se on s.sector_id = se.sector_id
left join client pr on pr.client_id = re.charterer_id
inner join container_group cg on ri.container_group_id = cg.container_group_id
inner join container_type ct on ct.container_type_id = cg.container_type_id
where ers.type != 'REJECTED' and pr.client_id in (:CRS) and ct.container_type_id in (:CT) and l.location_id in (:DS) and s.stand_id in (:AD) and se.sector_id in (:SE)

			  group by s.address, se.name, l.name, ct.name, pr.short_name) main
			  group by address, name, district, typeemkost, pr_name
)
			  

select  * from main1
union all
(select  
null,
null,
 null,
'Итого емкостей:' as address2, 
 null,
sum (rt.emkostzakaz) as emkostzakaz,
 sum (zakazvolume) as zakazvolume,
  sum (vivoz) as vivoz,
  sum (vivozvolume) as vivozvolume,
 sum (factvolume) as factvolume,
 null
from main1 rt)

union all
(select 
null,
null,
 null,
 null,
typeemkost,
sum (rt.emkostzakaz) as emkostzakaz,
 sum (zakazvolume) as zakazvolume,
  sum (vivoz) as vivoz,
  sum (vivozvolume) as vivozvolume,
 sum (factvolume) as factvolume,
 null
from main1 rt
group by typeemkost)

order by num