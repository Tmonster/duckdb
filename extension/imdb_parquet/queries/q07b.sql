SELECT MIN(n.name) AS of_person,
       MIN(t.title) AS biography_movie
FROM 'benchmark/imdb_parquet/data/aka_name.parquet' AS an,
     'benchmark/imdb_parquet/data/cast_info.parquet' AS ci,
     'benchmark/imdb_parquet/data/info_type.parquet' AS it,
     'benchmark/imdb_parquet/data/link_type.parquet' AS lt,
     'benchmark/imdb_parquet/data/movie_link.parquet' AS ml,
     'benchmark/imdb_parquet/data/name.parquet' AS n,
     'benchmark/imdb_parquet/data/person_info.parquet' AS pi,
     'benchmark/imdb_parquet/data/title.parquet' AS t
WHERE an.name LIKE '%a%'
     AND it.info ='mini biography'
     AND lt.link ='features'
     AND n.name_pcode_cf LIKE 'D%'
     AND n.gender='m'
     AND pi.note ='Volker Boehm'
     AND t.production_year BETWEEN 1980 AND 1984
     AND n.id = an.person_id
     AND n.id = pi.person_id
     AND ci.person_id = n.id
     AND t.id = ci.movie_id
     AND ml.linked_movie_id = t.id
     AND lt.id = ml.link_type_id
     AND it.id = pi.info_type_id
     AND pi.person_id = an.person_id
     AND pi.person_id = ci.person_id
     AND an.person_id = ci.person_id
     AND ci.movie_id = ml.linked_movie_id;
  