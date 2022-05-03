SELECT MIN(an1.name) AS actress_pseudonym,
       MIN(t.title) AS japanese_movie_dubbed
FROM 'benchmark/imdb_parquet/data/aka_name.parquet' AS an1,
     'benchmark/imdb_parquet/data/cast_info.parquet' AS ci,
     'benchmark/imdb_parquet/data/company_name.parquet' AS cn,
     'benchmark/imdb_parquet/data/movie_companies.parquet' AS mc,
     'benchmark/imdb_parquet/data/name.parquet' AS n1,
     'benchmark/imdb_parquet/data/role_type.parquet' AS rt,
     'benchmark/imdb_parquet/data/title.parquet' AS t
WHERE ci.note ='(voice: English version)'
     AND cn.country_code ='[jp]'
     AND mc.note LIKE '%(Japan)%'
     AND mc.note NOT LIKE '%(USA)%'
     AND n1.name LIKE '%Yo%'
     AND n1.name NOT LIKE '%Yu%'
     AND rt.role ='actress'
     AND an1.person_id = n1.id
     AND n1.id = ci.person_id
     AND ci.movie_id = t.id
     AND t.id = mc.movie_id
     AND mc.company_id = cn.id
     AND ci.role_id = rt.id
     AND an1.person_id = ci.person_id
     AND ci.movie_id = mc.movie_id;
  