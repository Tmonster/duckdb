SELECT MIN(t.title) AS movie_title
FROM 'benchmark/imdb_parquet/data/keyword.parquet' AS k,
     'benchmark/imdb_parquet/data/movie_info.parquet' AS mi,
     'benchmark/imdb_parquet/data/movie_keyword.parquet' AS mk,
     'benchmark/imdb_parquet/data/title.parquet' AS t
WHERE k.keyword LIKE '%sequel%'
     AND mi.info IN ('Bulgaria')
     AND t.production_year > 2010
     AND t.id = mi.movie_id
     AND t.id = mk.movie_id
     AND mk.movie_id = mi.movie_id
     AND k.id = mk.keyword_id;
  