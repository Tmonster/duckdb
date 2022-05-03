SELECT MIN(mi.info) AS movie_budget,
       MIN(mi_idx.info) AS movie_votes,
       MIN(t.title) AS movie_title
FROM 'benchmark/imdb_parquet/data/cast_info.parquet' AS ci,
     'benchmark/imdb_parquet/data/info_type.parquet' AS it1,
     'benchmark/imdb_parquet/data/info_type.parquet' AS it2,
     'benchmark/imdb_parquet/data/movie_info.parquet' AS mi,
     'benchmark/imdb_parquet/data/movie_info_idx.parquet' AS mi_idx,
     'benchmark/imdb_parquet/data/name.parquet' AS n,
     'benchmark/imdb_parquet/data/title.parquet' AS t
WHERE ci.note IN ('(producer)',
    '(executive producer)')
     AND it1.info = 'budget'
     AND it2.info = 'votes'
     AND n.gender = 'm'
     AND n.name LIKE '%Tim%'
     AND t.id = mi.movie_id
     AND t.id = mi_idx.movie_id
     AND t.id = ci.movie_id
     AND ci.movie_id = mi.movie_id
     AND ci.movie_id = mi_idx.movie_id
     AND mi.movie_id = mi_idx.movie_id
     AND n.id = ci.person_id
     AND it1.id = mi.info_type_id
     AND it2.id = mi_idx.info_type_id;
  