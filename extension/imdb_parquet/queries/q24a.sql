SELECT MIN(chn.name) AS voiced_char_name,
       MIN(n.name) AS voicing_actress_name,
       MIN(t.title) AS voiced_action_movie_jap_eng
FROM 'benchmark/imdb_parquet/data/aka_name.parquet' AS an,
     'benchmark/imdb_parquet/data/char_name.parquet' AS chn,
     'benchmark/imdb_parquet/data/cast_info.parquet' AS ci,
     'benchmark/imdb_parquet/data/company_name.parquet' AS cn,
     'benchmark/imdb_parquet/data/info_type.parquet' AS it,
     'benchmark/imdb_parquet/data/keyword.parquet' AS k,
     'benchmark/imdb_parquet/data/movie_companies.parquet' AS mc,
     'benchmark/imdb_parquet/data/movie_info.parquet' AS mi,
     'benchmark/imdb_parquet/data/movie_keyword.parquet' AS mk,
     'benchmark/imdb_parquet/data/name.parquet' AS n,
     'benchmark/imdb_parquet/data/role_type.parquet' AS rt,
     'benchmark/imdb_parquet/data/title.parquet' AS t
WHERE ci.note IN ('(voice)',
    '(voice: Japanese version)',
    '(voice) (uncredited)',
    '(voice: English version)')
     AND cn.country_code ='[us]'
     AND it.info = 'release dates'
     AND k.keyword IN ('hero',
      'martial-arts',
      'hand-to-hand-combat')
     AND mi.info IS NOT NULL
     AND (mi.info LIKE 'Japan:%201%'
OR mi.info LIKE 'USA:%201%')
     AND n.gender ='f'
     AND n.name LIKE '%An%'
     AND rt.role ='actress'
     AND t.production_year > 2010
     AND t.id = mi.movie_id
     AND t.id = mc.movie_id
     AND t.id = ci.movie_id
     AND t.id = mk.movie_id
     AND mc.movie_id = ci.movie_id
     AND mc.movie_id = mi.movie_id
     AND mc.movie_id = mk.movie_id
     AND mi.movie_id = ci.movie_id
     AND mi.movie_id = mk.movie_id
     AND ci.movie_id = mk.movie_id
     AND cn.id = mc.company_id
     AND it.id = mi.info_type_id
     AND n.id = ci.person_id
     AND rt.id = ci.role_id
     AND n.id = an.person_id
     AND ci.person_id = an.person_id
     AND chn.id = ci.person_role_id
     AND k.id = mk.keyword_id;
  