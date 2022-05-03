SELECT MIN(cn.name) AS movie_company,
       MIN(mi_idx.info) AS rating,
       MIN(t.title) AS complete_euro_dark_movie
FROM 'benchmark/imdb_parquet/data/complete_cast.parquet' AS cc,
     'benchmark/imdb_parquet/data/comp_cast_type.parquet' AS cct1,
     'benchmark/imdb_parquet/data/comp_cast_type.parquet' AS cct2,
     'benchmark/imdb_parquet/data/company_name.parquet' AS cn,
     'benchmark/imdb_parquet/data/company_type.parquet' AS ct,
     'benchmark/imdb_parquet/data/info_type.parquet' AS it1,
     'benchmark/imdb_parquet/data/info_type.parquet' AS it2,
     'benchmark/imdb_parquet/data/keyword.parquet' AS k,
     'benchmark/imdb_parquet/data/kind_type.parquet' AS kt,
     'benchmark/imdb_parquet/data/movie_companies.parquet' AS mc,
     'benchmark/imdb_parquet/data/movie_info.parquet' AS mi,
     'benchmark/imdb_parquet/data/movie_info_idx.parquet' AS mi_idx,
     'benchmark/imdb_parquet/data/movie_keyword.parquet' AS mk,
     'benchmark/imdb_parquet/data/title.parquet' AS t
WHERE cct1.kind = 'crew'
     AND cct2.kind != 'complete+verified'
     AND cn.country_code != '[us]'
     AND it1.info = 'countries'
     AND it2.info = 'rating'
     AND k.keyword IN ('murder',
      'murder-in-title',
      'blood',
      'violence')
     AND kt.kind IN ('movie',
    'episode')
     AND mc.note NOT LIKE '%(USA)%'
     AND mc.note LIKE '%(200%)%'
     AND mi.info IN ('Sweden',
    'Germany',
    'Swedish',
    'German')
     AND mi_idx.info > '6.5'
     AND t.production_year > 2005
     AND kt.id = t.kind_id
     AND t.id = mi.movie_id
     AND t.id = mk.movie_id
     AND t.id = mi_idx.movie_id
     AND t.id = mc.movie_id
     AND t.id = cc.movie_id
     AND mk.movie_id = mi.movie_id
     AND mk.movie_id = mi_idx.movie_id
     AND mk.movie_id = mc.movie_id
     AND mk.movie_id = cc.movie_id
     AND mi.movie_id = mi_idx.movie_id
     AND mi.movie_id = mc.movie_id
     AND mi.movie_id = cc.movie_id
     AND mc.movie_id = mi_idx.movie_id
     AND mc.movie_id = cc.movie_id
     AND mi_idx.movie_id = cc.movie_id
     AND k.id = mk.keyword_id
     AND it1.id = mi.info_type_id
     AND it2.id = mi_idx.info_type_id
     AND ct.id = mc.company_type_id
     AND cn.id = mc.company_id
     AND cct1.id = cc.subject_id
     AND cct2.id = cc.status_id;
  