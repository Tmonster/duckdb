SELECT MIN(k.keyword) as the_stuff
FROM --complete_cast AS cc,
     keyword AS k,
     movie_keyword as mk,
     cast_info AS ci
WHERE k.keyword IN ('superhero',
                    'sequel',
                    'second-part',
                    'marvel-comics',
                    'based-on-comic',
                    'tv-special',
                    'fight',
                    'violence')
AND k.id = mk.keyword_id
AND mk.movie_id = ci.movie_id;
-- AND ci.movie_id = cc.movie_id;

