SELECT MIN(lt.link) AS link_type,
       MIN(t1.title) AS first_movie,
       MIN(t2.title) AS second_movie
FROM 'benchmark/imdb_parquet/data/keyword.parquet' AS k,
     'benchmark/imdb_parquet/data/link_type.parquet' AS lt,
     'benchmark/imdb_parquet/data/movie_keyword.parquet' AS mk,
     'benchmark/imdb_parquet/data/movie_link.parquet' AS ml,
     'benchmark/imdb_parquet/data/title.parquet' AS t1,
     'benchmark/imdb_parquet/data/title.parquet' AS t2
WHERE k.keyword ='10,000-mile-club'
     AND mk.keyword_id = k.id
     AND t1.id = mk.movie_id
     AND ml.movie_id = t1.id
     AND ml.linked_movie_id = t2.id
     AND lt.id = ml.link_type_id
     AND mk.movie_id = t1.id;
  