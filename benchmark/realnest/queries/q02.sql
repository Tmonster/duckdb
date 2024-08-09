SELECT npvs, sum(x) AS sum_x, sum(y) AS sum_y, sum(z) AS sum_z
FROM (SELECT UNNEST(PV, recursive:=true) AS pv
      FROM Run2012B_SingleMu)
GROUP BY npvs;