SELECT MIN(chn.name) AS voiced_char,
       MIN(n.name) AS voicing_actress,
       MIN(t.title) AS voiced_animation
FROM 'benchmark/imdb_parquet/data/aka_name.parquet' AS an,
     'benchmark/imdb_parquet/data/complete_cast.parquet' AS cc,
     'benchmark/imdb_parquet/data/comp_cast_type.parquet' AS cct1,
     'benchmark/imdb_parquet/data/comp_cast_type.parquet' AS cct2,
     'benchmark/imdb_parquet/data/char_name.parquet' AS chn,
     'benchmark/imdb_parquet/data/cast_info.parquet' AS ci,
     'benchmark/imdb_parquet/data/company_name.parquet' AS cn,
     'benchmark/imdb_parquet/data/info_type.parquet' AS it,
     'benchmark/imdb_parquet/data/info_type.parquet' AS it3,
     'benchmark/imdb_parquet/data/keyword.parquet' AS k,
     'benchmark/imdb_parquet/data/movie_companies.parquet' AS mc,
     'benchmark/imdb_parquet/data/movie_info.parquet' AS mi,
     'benchmark/imdb_parquet/data/movie_keyword.parquet' AS mk,
     'benchmark/imdb_parquet/data/name.parquet' AS n,
     'benchmark/imdb_parquet/data/person_info.parquet' AS pi,
     'benchmark/imdb_parquet/data/role_type.parquet' AS rt,
     'benchmark/imdb_parquet/data/title.parquet' AS t
WHERE cct1.kind ='cast'
     AND cct2.kind ='complete+verified'
     AND chn.name = 'Queen'
     AND ci.note IN ('(voice)',
    '(voice) (uncredited)',
    '(voice: English version)')
     AND cn.country_code ='[us]'
     AND it.info = 'release dates'
     AND it3.info = 'height'
     AND k.keyword = 'computer-animation'
     AND mi.info LIKE 'USA:%200%'
     AND n.gender ='f'
     AND n.name LIKE '%An%'
     AND rt.role ='actress'
     AND t.title = 'Shrek 2'
     AND t.production_year BETWEEN 2000 AND 2005
     AND t.id = mi.movie_id
     AND t.id = mc.movie_id
     AND t.id = ci.movie_id
     AND t.id = mk.movie_id
     AND t.id = cc.movie_id
     AND mc.movie_id = ci.movie_id
     AND mc.movie_id = mi.movie_id
     AND mc.movie_id = mk.movie_id
     AND mc.movie_id = cc.movie_id
     AND mi.movie_id = ci.movie_id
     AND mi.movie_id = mk.movie_id
     AND mi.movie_id = cc.movie_id
     AND ci.movie_id = mk.movie_id
     AND ci.movie_id = cc.movie_id
     AND mk.movie_id = cc.movie_id
     AND cn.id = mc.company_id
     AND it.id = mi.info_type_id
     AND n.id = ci.person_id
     AND rt.id = ci.role_id
     AND n.id = an.person_id
     AND ci.person_id = an.person_id
     AND chn.id = ci.person_role_id
     AND n.id = pi.person_id
     AND ci.person_id = pi.person_id
     AND it3.id = pi.info_type_id
     AND k.id = mk.keyword_id
     AND cct1.id = cc.subject_id
     AND cct2.id = cc.status_id;
  