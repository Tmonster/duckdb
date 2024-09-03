SELECT
    count(*) as total_rows,
    count(*) FILTER (charge < 0) as negatives,
        count(*) FILTER (charge = 0) as positives,
        count(*) FILTER (charge > 0) as neutral,
        count(*) FILTER (pt - relIso_all < decayMode) as odds,
        count(*) FILTER (idIsoVLoose != idIsoLoose) as idIsoMatch
FROM (SELECT UNNEST(Tau, recursive:=true) AS tau FROM Run2012B_SingleMu)
;