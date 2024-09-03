SELECT unnested_hlt.*,
    unnested_pv.*,
    unnested_met.*,
    unnested_muon.*,
    unnested_electron.*,
    unnested_tau.*,
    unnested_photon.*,
    unnested_jet.*
    FROM unnested_hlt
    LEFT JOIN unnested_pv ON unnested_hlt.rowid = unnested_pv.rowid
    LEFT JOIN unnested_met ON unnested_hlt.rowid = unnested_met.rowid
    LEFT JOIN unnested_muon ON unnested_hlt.rowid = unnested_muon.rowid
    LEFT JOIN unnested_electron ON unnested_hlt.rowid = unnested_electron.rowid
    LEFT JOIN unnested_tau ON unnested_hlt.rowid = unnested_tau.rowid
    LEFT JOIN unnested_photon ON unnested_hlt.rowid = unnested_photon.rowid
    LEFT JOIN unnested_jet ON unnested_hlt.rowid = unnested_jet.rowid;