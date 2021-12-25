with Table_First as
(
select user_id,serviceid as FirstServiceid, date as FirstServiceDate from 
(SELECT user_id, serviceid, date , row_number() over(partition by user_id order by date) as rank_1
FROM sang.new_table) as view_1
where rank_1 =1
)


,Table_Second as
(select user_id, serviceid as SecondServiceid, date as SecondServiceDate from
(SELECT user_id, serviceid, date , row_number() over(partition by user_id order by date) as rank_2 FROM sang.new_table) as view_2
where rank_2 = 2
)



,Table_Last as 
(select view_3.user_id, view_3.serviceid as LastServiceid, view_3.date LastServiceDate, view_4.max_rank as TotalService
from (SELECT user_id, serviceid, date , row_number() over(partition by user_id order by date) as rank_ FROM sang.new_table) as view_3 
inner join (select user_id, max(rank_) max_rank from
(SELECT user_id, serviceid, date , row_number() over(partition by user_id order by date) as rank_ FROM sang.new_table) as view_0
group by user_id) as view_4 
on view_3.user_id = view_4.user_id
where rank_= max_rank)
, Nuniqueservice as
(SELECT user_id,  count(distinct serviceid) as TotalService from  sang.new_table
group by user_id)


select Table_First.user_id, Table_First.FirstServiceid, Table_First.FirstServiceDate, Table_Second.SecondServiceid, 
Table_Second.SecondServiceDate
, Table_Last.LastServiceid, Table_Last.LastServiceDate, Nuniqueservice.TotalService
 from Table_First left join Table_Second on Table_First.user_id = Table_Second.user_id left join Table_Last on 
Table_Second.user_id=Table_Last.user_id left join  Nuniqueservice on Table_Last.user_id = Nuniqueservice.user_id;