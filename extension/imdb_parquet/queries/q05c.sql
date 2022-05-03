SELECT MIN(t.title) AS american_movie
FROM 'benchmark/imdb_parquet/data/company_type.parquet' AS ct,
     'benchmark/imdb_parquet/data/info_type.parquet' AS it,
     'benchmark/imdb_parquet/data/movie_companies.parquet' AS mc,
     'benchmark/imdb_parquet/data/movie_info.parquet' AS mi,
     'benchmark/imdb_parquet/data/title.parquet' AS t
WHERE ct.kind = 'production companies'
     AND mc.note NOT LIKE '%(TV)%'
     AND mc.note LIKE '%(USA)%'
     AND mi.info IN ('Sweden',
    'Norway',
    'Germany',
    'Denmark',
    'Swedish',
    'Denish',
    'Norwegian',
    'German',
    'USA',
    'American')
     AND t.production_year > 1990
     AND t.id = mi.movie_id
     AND t.id = mc.movie_id
     AND mc.movie_id = mi.movie_id
     AND ct.id = mc.company_type_id
     AND it.id = mi.info_type_id;
  