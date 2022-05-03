SELECT MIN(an.name) AS cool_actor_pseudonym,
       MIN(t.title) AS series_named_after_char
FROM 'benchmark/imdb_parquet/data/aka_name.parquet' AS an,
     'benchmark/imdb_parquet/data/cast_info.parquet' AS ci,
     'benchmark/imdb_parquet/data/company_name.parquet' AS cn,
     'benchmark/imdb_parquet/data/keyword.parquet' AS k,
     'benchmark/imdb_parquet/data/movie_companies.parquet' AS mc,
     'benchmark/imdb_parquet/data/movie_keyword.parquet' AS mk,
     'benchmark/imdb_parquet/data/name.parquet' AS n,
     'benchmark/imdb_parquet/data/title.parquet' AS t
WHERE cn.country_code ='[us]'
     AND k.keyword ='character-name-in-title'
     AND t.episode_nr >= 50
     AND t.episode_nr < 100
     AND an.person_id = n.id
     AND n.id = ci.person_id
     AND ci.movie_id = t.id
     AND t.id = mk.movie_id
     AND mk.keyword_id = k.id
     AND t.id = mc.movie_id
     AND mc.company_id = cn.id
     AND an.person_id = ci.person_id
     AND ci.movie_id = mc.movie_id
     AND ci.movie_id = mk.movie_id
     AND mc.movie_id = mk.movie_id;
  