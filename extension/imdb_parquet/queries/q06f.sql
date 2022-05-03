SELECT MIN(k.keyword) AS movie_keyword,
       MIN(n.name) AS actor_name,
       MIN(t.title) AS hero_movie
FROM 'benchmark/imdb_parquet/data/cast_info.parquet' AS ci,
     'benchmark/imdb_parquet/data/keyword.parquet' AS k,
     'benchmark/imdb_parquet/data/movie_keyword.parquet' AS mk,
     'benchmark/imdb_parquet/data/name.parquet' AS n,
     'benchmark/imdb_parquet/data/title.parquet' AS t
WHERE k.keyword IN ('superhero',
      'sequel',
      'second-part',
      'marvel-comics',
      'based-on-comic',
      'tv-special',
      'fight',
      'violence')
     AND t.production_year > 2000
     AND k.id = mk.keyword_id
     AND t.id = mk.movie_id
     AND t.id = ci.movie_id
     AND ci.movie_id = mk.movie_id
     AND n.id = ci.person_id;
  