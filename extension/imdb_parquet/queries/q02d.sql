SELECT MIN(t.title) AS movie_title
FROM 'benchmark/imdb_parquet/data/company_name.parquet' AS cn,
     'benchmark/imdb_parquet/data/keyword.parquet' AS k,
     'benchmark/imdb_parquet/data/movie_companies.parquet' AS mc,
     'benchmark/imdb_parquet/data/movie_keyword.parquet' AS mk,
     'benchmark/imdb_parquet/data/title.parquet' AS t
WHERE cn.country_code ='[us]'
     AND k.keyword ='character-name-in-title'
     AND cn.id = mc.company_id
     AND mc.movie_id = t.id
     AND t.id = mk.movie_id
     AND mk.keyword_id = k.id
     AND mc.movie_id = mk.movie_id;
  