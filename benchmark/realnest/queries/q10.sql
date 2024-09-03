select keys, count(*) mentions from
    (select unnest(map_keys(tags)) as keys from wat)
group by keys
order by mentions
desc limit 20;