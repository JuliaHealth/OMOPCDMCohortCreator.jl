"""
GenerateCohorts(
    conn;
    visit_codes = nothing,
    condition_codes = nothing,
    race_codes = nothing,
    state_codes = nothing,
    gender_codes = nothing,
    age_groupings = nothing,
)

Get all unique `person_id`'s matching given cohort criterion filters based on the keyword arguments.
The keyword arguments act as filters that will build cohorts filtered to match given codes.

# Arguments:

`conn` - database connection using DBInterface

# Keyword Arguments:

- `visit_codes` - a vector of `visit_concept_id`'s; must be a subtype of `Integer`. Default: `nothing`
- `condition_codes` - a vector of `condition_concept_id`'s; must be a subtype of `Integer`. Default: `nothing`
- `race_codes` - a vector of `race_concept_id`'s; must be a subtype of `Integer`. Default: `nothing`
- `state_codes` - a vector of `state`'s; must be a subtype of `AbstractString`. Default: `nothing`
- `gender_codes` - a vector of `gender_concept_id`'s; must be a subtype of `Integer`. Default: `nothing`
- `age_groupings` - a vector of age groups of the form `[[10, 19], [20, 29],]` denoting an age group of 10 - 19 and 20 - 29 respectively; age values must subtype of `Integer`. Default: `nothing`

# Returns

- `ids::Vector{Int}` - the list of persons matching a given cohort based on key word argument filters
"""
function GenerateCohorts(
    conn;
    visit_codes = nothing,
    condition_codes = nothing,
    race_codes = nothing,
    state_codes = nothing,
    gender_codes = nothing,
    age_groupings = nothing,
)
    filter_list = []

    !isnothing(visit_codes) && push!(filter_list, VisitFilterPersonIDs(visit_codes, conn)) 
    !isnothing(condition_codes) &&
        push!(filter_list, ConditionFilterPersonIDs(condition_codes, conn))
    !isnothing(race_codes) && push!(filter_list, RaceFilterPersonIDs(race_codes, conn))
    !isnothing(state_codes) &&
        push!(filter_list, StateFilterPersonIDs(state_codes, conn))
    !isnothing(gender_codes) &&
        push!(filter_list, GenderFilterPersonIDs(gender_codes, conn))

    !isnothing(age_groupings[1]) &&
        push!(filter_list, AgeGroupFilterPersonIDs(age_groupings, conn))

    return isempty(filter_list) ? filter_list : convert(Vector{Int}, intersect(filter_list...))

end

"""
GenerateStudyPopulation(
    cohort_ids,
    conn;
    by_visit = false,
    by_state = false,
    by_gender = false,
    by_race = false,
    by_age_group = false,
)

Stratify a given list of `person_id`'s or "cohort" by key word arguments.

# Arguments:

- `cohort_ids::Vector{Int}` - a list of `person_id`'s representing a cohort to be stratified
- `conn` - database connection using DBInterface

# Keyword Arguments:

`by_visit` - stratify the cohort by visit type?  Default: `false`
`by_state` - stratify the cohort by state location? Default: `false`
`by_gender` - stratify the cohort by gender? Default: `false`
`by_race` - stratify the cohort by race? Default: `false`
`by_age_group` - stratify the cohort by age groups (defaults to breakdown from `GetPatientAgeGroup`)? Default: `false`

"""
function GenerateStudyPopulation(
    cohort_ids,
    conn;
    by_visit = false,
    by_state = false,
    by_gender = false,
    by_race = false,
    by_age_group = false,
)

    characteristics = Dict()

    by_visit && println("Not implemented yet!") # push!(characteristics, :visit => GetPatientVisits(cohort_ids, conn))
    #TODO: Implement VisitPatientVisits
    by_state && push!(characteristics, :state => GetPatientState(cohort_ids, conn))
    by_gender && push!(characteristics, :gender => GetPatientGender(cohort_ids, conn))
    by_race && push!(characteristics, :race => GetPatientRace(cohort_ids, conn))
    by_age_group &&
        push!(characteristics, :age_group => GetPatientAgeGroup(cohort_ids, conn))

    df = DataFrame(:person_id => cohort_ids)
    for feature in keys(characteristics)
        df = innerjoin(df, characteristics[feature], on = :person_id)
    end

    return df

end

"""
GenerateGroupCounts(data::DataFrame)

Given data in a DataFrame, get group counts based on each feature found in the DataFrame and removes `person_id` for privacy aggregation purposes.

# Arguments:

- `data::DataFrame` - a DataFrame that must have at least a `person_id` column

# Returns:

- `df::DataFrame` - a DataFrame that contains the group counts based on each feature found in `data` with the `person_id` field removed for privacy
"""
function GenerateGroupCounts(data::DataFrame)
    cols = filter(x -> x != :person_id, propertynames(data))
    df = groupby(data, cols) |> x -> combine(x, nrow => :count)

    return df

end

"""
GenerateDatabaseDetails(dialect::Symbol, schema::String)

Generates the dialect and schema details for accessing a given OMOP CDM database.

# Arguments:

- `dialect::Symbol` - the dialect used for SQL queries (to see what is dialects are available, see here: https://mechanicalrabbit.github.io/FunSQL.jl/stable/reference/#FunSQL.SQLDialect)
- `schema::String` - the name of the database schema being used.
"""
function GenerateDatabaseDetails(dialect, schema)
    @eval global dialect = $(QuoteNode(dialect))
    @eval global schema = $(QuoteNode(schema))

    @info "Global database dialect set to: $dialect"
    @info "Global schema set to: $schema"

    return nothing
end

"""
GenerateTables(conn; inplace = true, exported = false)

Generates Julia representations of all tables found in a given OMOP CDM database.

# Arguments:

- `dialect::Symbol` - the dialect used for SQL queries (to see what is dialects are available, see here: https://mechanicalrabbit.github.io/FunSQL.jl/stable/reference/#FunSQL.SQLDialect)
- `schema::String` - the name of the database schema being used.
"""
function GenerateTables(conn; inplace = true, exported = false)

    db_info = reflect(conn; schema = schema, dialect = dialect)

    if inplace == true
        for key in keys(db_info.tables)
            @eval global $(Symbol(lowercase(string(key)))) = $(db_info[key])
            @info "$(lowercase(string(key))) table generated internally"
        end

    end

    if exported == true
        tables = Dict()
        for key in keys(db_info.tables)
            push!(tables, Symbol(lowercase(string(key))) => db_info[key])
            @info "$(lowercase(string(key))) table generated publicly"
        end

        return tables

    end

    return conn

end
#NOTE: Create workaround for case matching across SQL flavors if Casing proposal fails: https://github.com/OHDSI/CommonDataModel/issues/509

export 
    GenerateDatabaseDetails, GenerateGroupCounts, GenerateTables
