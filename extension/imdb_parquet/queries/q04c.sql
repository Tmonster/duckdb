SELECT MIN(mi_idx.info) AS rating,
       MIN(t.title) AS movie_title
FROM 'benchmark/imdb_parquet/data/info_type.parquet' AS it,
     'benchmark/imdb_parquet/data/keyword.parquet' AS k,
     'benchmark/imdb_parquet/data/movie_info_idx.parquet' AS mi_idx,
     'benchmark/imdb_parquet/data/movie_keyword.parquet' AS mk,
     'benchmark/imdb_parquet/data/title.parquet' AS t
WHERE it.info ='rating'
     AND k.keyword LIKE '%sequel%'
     AND mi_idx.info > '2.0'
     AND t.production_year > 1990
     AND t.id = mi_idx.movie_id
     AND t.id = mk.movie_id
     AND mk.movie_id = mi_idx.movie_id
     AND k.id = mk.keyword_id
     AND it.id = mi_idx.info_type_id;
  