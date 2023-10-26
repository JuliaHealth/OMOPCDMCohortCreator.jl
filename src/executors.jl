"""
ExecuteAudit(data::DataFrame; hitech = true)

Function that executes an audit on a dataframe that must contain a `count` column to ensure compliance with auditing and privacy preserving best practices

# Arguments:

- `data::DataFrame` - the data to audit that must be in a `DataFrame` and contain a column called `count`

# Keyword Arguments:

- `hitech::Bool` - a boolean that enforces HITECH standards for privacy preserving methods.

- `target_column::Symbol` - the name of the column to target for auditing (default set to :count).

# Returns

- `df` - a `DataFrame` that is appropriately audited per a given standard
"""
function ExecuteAudit(data::DataFrame; hitech = true, target_column = :count)
    df = hitech && filter(row -> row.count >= 11, data)

    return df

end

# ASSUMPTION: The Database follows the OMOP CDM 5.4 Schema completely

export ExecuteAudit
