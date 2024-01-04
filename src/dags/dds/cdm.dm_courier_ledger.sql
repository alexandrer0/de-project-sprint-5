delete from cdm.dm_courier_ledger
    where settlement_year = EXTRACT(YEAR FROM '{{ds}}'::date) and settlement_month = EXTRACT(MONTH FROM '{{ds}}'::date);
insert into cdm.dm_courier_ledger (
    courier_id,
    courier_name,
    settlement_year,
    settlement_month,
    orders_count,
    orders_total_sum,
    rate_avg,
    order_processing_fee,
    courier_order_sum,
    courier_tips_sum,
    courier_reward_sum
)
select
	dc.courier_id,
	dc.courier_name,
	EXTRACT(YEAR FROM order_ts) settlement_year,
	EXTRACT(MONTH FROM order_ts) settlement_month,
	count(order_id) orders_count,
	sum(total_sum) orders_total_sum,
	avg(rate) rate_avg,
	sum(total_sum)*0.25 order_processing_fee,
	case
		when avg(rate)<4 then sum(greatest(0.05*total_sum,100))
		when avg(rate)<4.5 then sum(greatest(0.07*total_sum,150))
		when avg(rate)<4.9 then sum(greatest(0.08*total_sum,175))
		else sum(greatest(0.1*total_sum,200))
	end courier_order_sum,
	sum(tip_sum) courier_tips_sum,
	case
		when avg(rate)<4 then sum(greatest(0.05*total_sum,100))
		when avg(rate)<4.5 then sum(greatest(0.07*total_sum,150))
		when avg(rate)<4.9 then sum(greatest(0.08*total_sum,175))
		else sum(greatest(0.1*total_sum,200))
	end+sum(tip_sum)*0.95 courier_reward_sum
from dds.fct_deliveries fd
left join dds.dm_couriers dc on fd.courier_id = dc.id
where date_trunc('month', fd.order_ts) = date_trunc('month', '{{ds}}'::date)
group by 1,2,3,4