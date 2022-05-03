SELECT MIN(mi.info) AS release_date,
       MIN(t.title) AS modern_american_internet_movie
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
     AND it1.info = 'release dates'
     AND mi.note LIKE '%internet%'
     AND mi.info IS NOT NULL
     AND (mi.info LIKE 'USA:% 199%'
OR mi.info LIKE 'USA:% 200%')
     AND t.production_year > 1990
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
  