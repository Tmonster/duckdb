SELECT MIN(cn.name) AS movie_company,
       MIN(mi_idx.info) AS rating,
       MIN(t.title) AS drama_horror_movie
FROM 'benchmark/imdb_parquet/data/company_name.parquet' AS cn,
     'benchmark/imdb_parquet/data/company_type.parquet' AS ct,
     'benchmark/imdb_parquet/data/info_type.parquet' AS it1,
     'benchmark/imdb_parquet/data/info_type.parquet' AS it2,
     'benchmark/imdb_parquet/data/movie_companies.parquet' AS mc,
     'benchmark/imdb_parquet/data/movie_info.parquet' AS mi,
     'benchmark/imdb_parquet/data/movie_info_idx.parquet' AS mi_idx,
     'benchmark/imdb_parquet/data/title.parquet' AS t
WHERE cn.country_code = '[us]'
     AND ct.kind = 'production companies'
     AND it1.info = 'genres'
     AND it2.info = 'rating'
     AND mi.info IN ('Drama',
    'Horror')
     AND mi_idx.info > '8.0'
     AND t.production_year BETWEEN 2005 AND 2008
     AND t.id = mi.movie_id
     AND t.id = mi_idx.movie_id
     AND mi.info_type_id = it1.id
     AND mi_idx.info_type_id = it2.id
     AND t.id = mc.movie_id
     AND ct.id = mc.company_type_id
     AND cn.id = mc.company_id
     AND mc.movie_id = mi.movie_id
     AND mc.movie_id = mi_idx.movie_id
     AND mi.movie_id = mi_idx.movie_id;
  