SELECT MIN(mi.info) AS release_date,
       MIN(t.title) AS youtube_movie
FROM 'benchmark/imdb_parquet/data/aka_title.parquet' AS at,
     'benchmark/imdb_parquet/data/company_name.parquet' AS cn,
     'benchmark/imdb_parquet/data/company_type.parquet' AS ct,
     'benchmark/imdb_parquet/data/info_type.parquet' AS it1,
     'benchmark/imdb_parquet/data/keyword.parquet' AS k,
     'benchmark/imdb_parquet/data/movie_companies.parquet' AS mc,
     'benchmark/imdb_parquet/data/movie_info.parquet' AS mi,
     'benchmark/imdb_parquet/data/movie_keyword.parquet' AS mk,
     'benchmark/imdb_parquet/data/title.parquet' AS t
WHERE cn.country_code = '[us]'
     AND cn.name = 'YouTube'
     AND it1.info = 'release dates'
     AND mc.note LIKE '%(200%)%'
     AND mc.note LIKE '%(worldwide)%'
     AND mi.note LIKE '%internet%'
     AND mi.info LIKE 'USA:% 200%'
     AND t.production_year BETWEEN 2005 AND 2010
     AND t.id = at.movie_id
     AND t.id = mi.movie_id
     AND t.id = mk.movie_id
     AND t.id = mc.movie_id
     AND mk.movie_id = mi.movie_id
     AND mk.movie_id = mc.movie_id
     AND mk.movie_id = at.movie_id
     AND mi.movie_id = mc.movie_id
     AND mi.movie_id = at.movie_id
     AND mc.movie_id = at.movie_id
     AND k.id = mk.keyword_id
     AND it1.id = mi.info_type_id
     AND cn.id = mc.company_id
     AND ct.id = mc.company_type_id;
  