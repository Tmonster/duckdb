SELECT puId, avg(pt), avg(eta), avg(phi), avg(mass), avg(btag)
FROM (
         SELECT UNNEST(Jet, recursive:=true) AS unnested_jet
         FROM Run2012B_SingleMu
     )
GROUP BY puId
HAVING avg(mass) > avg(eta)
;