SELECT PV.npvs, sum(PV.x) AS sum_x, sum(PV.y) AS sum_y, sum(PV.z) AS sum_z
FROM Run2012B_SingleMu
GROUP BY PV.npvs;