SELECT MIN(mi.info) AS movie_budget,
       MIN(mi_idx.info) AS movie_votes,
       MIN(n.name) AS writer,
       MIN(t.title) AS violent_liongate_movie
FROM 'benchmark/imdb_parquet/data/cast_info.parquet' AS ci,
     'benchmark/imdb_parquet/data/company_name.parquet' AS cn,
     'benchmark/imdb_parquet/data/info_type.parquet' AS it1,
     'benchmark/imdb_parquet/data/info_type.parquet' AS it2,
     'benchmark/imdb_parquet/data/keyword.parquet' AS k,
     'benchmark/imdb_parquet/data/movie_companies.parquet' AS mc,
     'benchmark/imdb_parquet/data/movie_info.parquet' AS mi,
     'benchmark/imdb_parquet/data/movie_info_idx.parquet' AS mi_idx,
     'benchmark/imdb_parquet/data/movie_keyword.parquet' AS mk,
     'benchmark/imdb_parquet/data/name.parquet' AS n,
     'benchmark/imdb_parquet/data/title.parquet' AS t
WHERE ci.note IN ('(writer)',
    '(head writer)',
    '(written by)',
    '(story)',
    '(story editor)')
     AND cn.name LIKE 'Lionsgate%'
     AND it1.info = 'genres'
     AND it2.info = 'votes'
     AND k.keyword IN ('murder',
      'violence',
      'blood',
      'gore',
      'death',
      'female-nudity',
      'hospital')
     AND mc.note LIKE '%(Blu-ray)%'
     AND mi.info IN ('Horror',
    'Thriller')
     AND n.gender = 'm'
     AND t.production_year > 2000
     AND (t.title LIKE '%Freddy%'
OR t.title LIKE '%Jason%'
OR t.title LIKE 'Saw%')
     AND t.id = mi.movie_id
     AND t.id = mi_idx.movie_id
     AND t.id = ci.movie_id
     AND t.id = mk.movie_id
     AND t.id = mc.movie_id
     AND ci.movie_id = mi.movie_id
     AND ci.movie_id = mi_idx.movie_id
     AND ci.movie_id = mk.movie_id
     AND ci.movie_id = mc.movie_id
     AND mi.movie_id = mi_idx.movie_id
     AND mi.movie_id = mk.movie_id
     AND mi.movie_id = mc.movie_id
     AND mi_idx.movie_id = mk.movie_id
     AND mi_idx.movie_id = mc.movie_id
     AND mk.movie_id = mc.movie_id
     AND n.id = ci.person_id
     AND it1.id = mi.info_type_id
     AND it2.id = mi_idx.info_type_id
     AND k.id = mk.keyword_id
     AND cn.id = mc.company_id;
  