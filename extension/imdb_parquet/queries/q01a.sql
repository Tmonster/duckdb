SELECT MIN(mc.note) AS production_note,
       MIN(t.title) AS movie_title,
       MIN(t.production_year) AS movie_year
FROM 'benchmark/imdb_parquet/data/company_type.parquet' AS ct,
     'benchmark/imdb_parquet/data/info_type.parquet' AS it,
     'benchmark/imdb_parquet/data/movie_companies.parquet' AS mc,
     'benchmark/imdb_parquet/data/movie_info_idx.parquet' AS mi_idx,
     'benchmark/imdb_parquet/data/title.parquet' AS t
WHERE ct.kind = 'production companies'
     AND it.info = 'top 250 rank'
     AND mc.note NOT LIKE '%(as Metro-Goldwyn-Mayer Pictures)%'
     AND (mc.note LIKE '%(co-production)%'
OR mc.note LIKE '%(presents)%')
     AND ct.id = mc.company_type_id
     AND t.id = mc.movie_id
     AND t.id = mi_idx.movie_id
     AND mc.movie_id = mi_idx.movie_id
     AND it.id = mi_idx.info_type_id;
  