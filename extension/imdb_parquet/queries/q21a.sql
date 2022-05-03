SELECT MIN(cn.name) AS company_name,
       MIN(lt.link) AS link_type,
       MIN(t.title) AS western_follow_up
FROM 'benchmark/imdb_parquet/data/company_name.parquet' AS cn,
     'benchmark/imdb_parquet/data/company_type.parquet' AS ct,
     'benchmark/imdb_parquet/data/keyword.parquet' AS k,
     'benchmark/imdb_parquet/data/link_type.parquet' AS lt,
     'benchmark/imdb_parquet/data/movie_companies.parquet' AS mc,
     'benchmark/imdb_parquet/data/movie_info.parquet' AS mi,
     'benchmark/imdb_parquet/data/movie_keyword.parquet' AS mk,
     'benchmark/imdb_parquet/data/movie_link.parquet' AS ml,
     'benchmark/imdb_parquet/data/title.parquet' AS t
WHERE cn.country_code !='[pl]'
     AND (cn.name LIKE '%Film%'
OR cn.name LIKE '%Warner%')
     AND ct.kind ='production companies'
     AND k.keyword ='sequel'
     AND lt.link LIKE '%follow%'
     AND mc.note IS NULL
     AND mi.info IN ('Sweden',
    'Norway',
    'Germany',
    'Denmark',
    'Swedish',
    'Denish',
    'Norwegian',
    'German')
     AND t.production_year BETWEEN 1950 AND 2000
     AND lt.id = ml.link_type_id
     AND ml.movie_id = t.id
     AND t.id = mk.movie_id
     AND mk.keyword_id = k.id
     AND t.id = mc.movie_id
     AND mc.company_type_id = ct.id
     AND mc.company_id = cn.id
     AND mi.movie_id = t.id
     AND ml.movie_id = mk.movie_id
     AND ml.movie_id = mc.movie_id
     AND mk.movie_id = mc.movie_id
     AND ml.movie_id = mi.movie_id
     AND mk.movie_id = mi.movie_id
     AND mc.movie_id = mi.movie_id;
  