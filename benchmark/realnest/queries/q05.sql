SELECT list_filter(
           [pt, eta, phi, mass, pfRelIso03_all, pfRelIso04_all, dxy, dxyErr, jetIdx, genPartIdx],
                x -> x > 0.01)
FROM (
         SELECT UNNEST(Muon, recursive:=true) AS unnested_muon
         FROM Run2012B_SingleMu
     )
;