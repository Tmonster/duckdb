SELECT MIN(an1.name) AS costume_designer_pseudo,
       MIN(t.title) AS movie_with_costumes
FROM 'benchmark/imdb_parquet/data/aka_name.parquet' AS an1,
     'benchmark/imdb_parquet/data/cast_info.parquet' AS ci,
     'benchmark/imdb_parquet/data/company_name.parquet' AS cn,
     'benchmark/imdb_parquet/data/movie_companies.parquet' AS mc,
     'benchmark/imdb_parquet/data/name.parquet' AS n1,
     'benchmark/imdb_parquet/data/role_type.parquet' AS rt,
     'benchmark/imdb_parquet/data/title.parquet' AS t
WHERE cn.country_code ='[us]'
     AND rt.role ='costume designer'
     AND an1.person_id = n1.id
     AND n1.id = ci.person_id
     AND ci.movie_id = t.id
     AND t.id = mc.movie_id
     AND mc.company_id = cn.id
     AND ci.role_id = rt.id
     AND an1.person_id = ci.person_id
     AND ci.movie_id = mc.movie_id;
  