set @date = "2020-01-07 00:00:00";/*comment 1*/
select date(required_by) as date //comment 2
, client_party_id as client_id /*comment 3*/
, ship_from_location_party_id as wh_id --comment 4
, channel_id as channel #comment 5
, sum(IF(d.manifest_date is not null, IF (d.manifest_date > a.required_by,0,ordered_quantity), IF (@date > a.required_by,0,ordered_quantity))) as sla_met
, sum(IF(d.manifest_date is not null, IF (d.manifest_date > a.required_by,ordered_quantity,0), IF (@date > a.required_by,ordered_quantity,0))) as sla_breached
from oms.oms_orders a #comment 6
left join (select order_id
, timestamp(case when c.status = "CLOSED" then c.updated_at else NULL end) as manifest_date 
from oms.oms_shipments b
left join oms.oms_manifests c
on b.manifest_id = c.id) d
on a.id = d.order_id
left join oms.oms_order_items e
on a.id = e.order_id
where flow = "OUTWARD" and pack_type != "BULK" and date(a.required_by) = date(@date)
group by 1,2,3,4