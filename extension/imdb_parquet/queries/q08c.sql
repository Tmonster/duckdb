SELECT MIN(a1.name) AS writer_pseudo_name,
       MIN(t.title) AS movie_title
FROM 'benchmark/imdb_parquet/data/aka_name.parquet' AS a1,
     'benchmark/imdb_parquet/data/cast_info.parquet' AS ci,
     'benchmark/imdb_parquet/data/company_name.parquet' AS cn,
     'benchmark/imdb_parquet/data/movie_companies.parquet' AS mc,
     'benchmark/imdb_parquet/data/name.parquet' AS n1,
     'benchmark/imdb_parquet/data/role_type.parquet' AS rt,
     'benchmark/imdb_parquet/data/title.parquet' AS t
WHERE cn.country_code ='[us]'
     AND rt.role ='writer'
     AND a1.person_id = n1.id
     AND n1.id = ci.person_id
     AND ci.movie_id = t.id
     AND t.id = mc.movie_id
     AND mc.company_id = cn.id
     AND ci.role_id = rt.id
     AND a1.person_id = ci.person_id
     AND ci.movie_id = mc.movie_id;
  