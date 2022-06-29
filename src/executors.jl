"""
TODO: Add docs when ready
"""
function ExecuteAudit(data::DataFrame; hitech = true)
    df = hitech && filter(row -> row.count >= 11, data)

    return df

end

# ASSUMPTION: The Database follows the OMOP CDM 5.4 Schema completely

export ExecuteAudit
