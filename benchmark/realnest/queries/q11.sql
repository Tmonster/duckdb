select hashtags, count(*) as num_mentions from (select unnest(list_hashtags) as hashtags from (select list_transform(data.entities.hashtags, x -> x.tag) as list_hashtags from wat where len(list_hashtags) != 0)) group by hashtags order by num_mentions desc limit 20;

-- select count(*) as num_nfl_mentions from (select list_transform(data.entities.hashtags, x -> x.tag) as list_hashtags, len(list_filter(list_hashtags, x -> x ilike '%nfl%')) as contains_nfl from wat where len(list_hashtags) != 0 and contains_nfl > 0);

-- select data.text, list_transform(data.entities.hashtags, x -> x.tag) as list_hashtags, len(list_filter(list_hashtags, x -> x ilike '%nfl%')) as contains_nfl from wat where len(list_hashtags) != 0 and contains_nfl > 0;

