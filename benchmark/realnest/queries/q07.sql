SELECT charge, sum(pt), sum(eta), sum(phi), sum(mass)
FROM (
         SELECT UNNEST(Muon, recursive:=true) AS unnested_muon
         FROM Run2012B_SingleMu
     )
GROUP BY charge
;