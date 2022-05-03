SELECT MIN(chn.name) AS character_name,
       MIN(mi_idx.info) AS rating,
       MIN(n.name) AS playing_actor,
       MIN(t.title) AS complete_hero_movie
FROM 'benchmark/imdb_parquet/data/complete_cast.parquet' AS cc,
     'benchmark/imdb_parquet/data/comp_cast_type.parquet' AS cct1,
     'benchmark/imdb_parquet/data/comp_cast_type.parquet' AS cct2,
     'benchmark/imdb_parquet/data/char_name.parquet' AS chn,
     'benchmark/imdb_parquet/data/cast_info.parquet' AS ci,
     'benchmark/imdb_parquet/data/info_type.parquet' AS it2,
     'benchmark/imdb_parquet/data/keyword.parquet' AS k,
     'benchmark/imdb_parquet/data/kind_type.parquet' AS kt,
     'benchmark/imdb_parquet/data/movie_info_idx.parquet' AS mi_idx,
     'benchmark/imdb_parquet/data/movie_keyword.parquet' AS mk,
     'benchmark/imdb_parquet/data/name.parquet' AS n,
     'benchmark/imdb_parquet/data/title.parquet' AS t
WHERE cct1.kind = 'cast'
     AND cct2.kind LIKE '%complete%'
     AND chn.name IS NOT NULL
     AND (chn.name LIKE '%man%'
OR chn.name LIKE '%Man%')
     AND it2.info = 'rating'
     AND k.keyword IN ('superhero',
      'marvel-comics',
      'based-on-comic',
      'tv-special',
      'fight',
      'violence',
      'magnet',
      'web',
      'claw',
      'laser')
     AND kt.kind = 'movie'
     AND mi_idx.info > '7.0'
     AND t.production_year > 2000
     AND kt.id = t.kind_id
     AND t.id = mk.movie_id
     AND t.id = ci.movie_id
     AND t.id = cc.movie_id
     AND t.id = mi_idx.movie_id
     AND mk.movie_id = ci.movie_id
     AND mk.movie_id = cc.movie_id
     AND mk.movie_id = mi_idx.movie_id
     AND ci.movie_id = cc.movie_id
     AND ci.movie_id = mi_idx.movie_id
     AND cc.movie_id = mi_idx.movie_id
     AND chn.id = ci.person_role_id
     AND n.id = ci.person_id
     AND k.id = mk.keyword_id
     AND cct1.id = cc.subject_id
     AND cct2.id = cc.status_id
     AND it2.id = mi_idx.info_type_id;
  