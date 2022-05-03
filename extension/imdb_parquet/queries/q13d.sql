SELECT MIN(cn.name) AS producing_company,
       MIN(miidx.info) AS rating,
       MIN(t.title) AS movie
FROM 'benchmark/imdb_parquet/data/company_name.parquet' AS cn,
     'benchmark/imdb_parquet/data/company_type.parquet' AS ct,
     'benchmark/imdb_parquet/data/info_type.parquet' AS it,
     'benchmark/imdb_parquet/data/info_type.parquet' AS it2,
     'benchmark/imdb_parquet/data/kind_type.parquet' AS kt,
     'benchmark/imdb_parquet/data/movie_companies.parquet' AS mc,
     'benchmark/imdb_parquet/data/movie_info.parquet' AS mi,
     'benchmark/imdb_parquet/data/movie_info_idx.parquet' AS miidx,
     'benchmark/imdb_parquet/data/title.parquet' AS t
WHERE cn.country_code ='[us]'
     AND ct.kind ='production companies'
     AND it.info ='rating'
     AND it2.info ='release dates'
     AND kt.kind ='movie'
     AND mi.movie_id = t.id
     AND it2.id = mi.info_type_id
     AND kt.id = t.kind_id
     AND mc.movie_id = t.id
     AND cn.id = mc.company_id
     AND ct.id = mc.company_type_id
     AND miidx.movie_id = t.id
     AND it.id = miidx.info_type_id
     AND mi.movie_id = miidx.movie_id
     AND mi.movie_id = mc.movie_id
     AND miidx.movie_id = mc.movie_id;
  