SELECT MIN(an.name) AS alternative_name,
       MIN(chn.name) AS voiced_character,
       MIN(n.name) AS voicing_actress,
       MIN(t.title) AS american_movie
FROM 'benchmark/imdb_parquet/data/aka_name.parquet' AS an,
     'benchmark/imdb_parquet/data/char_name.parquet' AS chn,
     'benchmark/imdb_parquet/data/cast_info.parquet' AS ci,
     'benchmark/imdb_parquet/data/company_name.parquet' AS cn,
     'benchmark/imdb_parquet/data/movie_companies.parquet' AS mc,
     'benchmark/imdb_parquet/data/name.parquet' AS n,
     'benchmark/imdb_parquet/data/role_type.parquet' AS rt,
     'benchmark/imdb_parquet/data/title.parquet' AS t
WHERE ci.note = '(voice)'
     AND cn.country_code ='[us]'
     AND mc.note LIKE '%(200%)%'
     AND (mc.note LIKE '%(USA)%'
OR mc.note LIKE '%(worldwide)%')
     AND n.gender ='f'
     AND n.name LIKE '%Angel%'
     AND rt.role ='actress'
     AND t.production_year BETWEEN 2007 AND 2010
     AND ci.movie_id = t.id
     AND t.id = mc.movie_id
     AND ci.movie_id = mc.movie_id
     AND mc.company_id = cn.id
     AND ci.role_id = rt.id
     AND n.id = ci.person_id
     AND chn.id = ci.person_role_id
     AND an.person_id = n.id
     AND an.person_id = ci.person_id;
  