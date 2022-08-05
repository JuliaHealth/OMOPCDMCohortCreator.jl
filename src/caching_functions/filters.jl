"""
VisitFilterPersonIDs(visit_codes, conn; tab::SQLTable = visit_occurrence)

Given a list of visit concept IDs, `visit_codes` return from the database patients matching at least one of the provided visit codes from the Visit Occurrence table.

# Arguments:

- `visit_codes` - a vector of `visit_concept_id`'s; must be a subtype of `Integer`

`conn` - database connection using DBInterface

# Keyword Arguments:

- `tab::SQLTable` - the `SQLTable` representing the Visit Occurrence table; default `visit_occurrence`

# Returns

- `ids::Vector{Int64}` - the list of persons resulting from the filter
"""
@memoize Dict function VisitFilterPersonIDs(visit_codes, conn; tab::SQLTable = visit_occurrence)
    ids =
        From(tab) |>
        Where(Fun.in(Get.visit_concept_id, visit_codes...)) |>
        Group(Get.person_id) |>
        q -> render(q, dialect = dialect) |>
        x -> DBInterface.execute(conn, String(x)) |> DataFrame

    return convert(Vector{Int}, ids.person_id)

end

"""
ConditionFilterPersonIDs(condition_codes, conn; tab::SQLTable = condition_occurrence)

Given a list of condition concept IDs, `condition_codes`, return from the database individuals having at least one entry in the Condition Occurrence table matching at least one of the provided condition types.

# Arguments:

- `condition_codes` - a vector of `condition_concept_id`'s; must be a subtype of `Integer`

`conn` - database connection using DBInterface

# Keyword Arguments:

- `tab::SQLTable` - the `SQLTable` representing the Condition Occurrence table; default `condition_occurrence`

# Returns

- `ids::Vector{Int64}` - the list of persons resulting from the filter
"""
@memoize Dict function ConditionFilterPersonIDs(
    condition_codes, conn;
    tab = condition_occurrence,
)
    ids =
        From(tab) |>
        Where(Fun.in(Get.condition_concept_id, condition_codes...)) |>
        Group(Get.person_id) |>
        q -> render(q, dialect = dialect) |>
        x -> DBInterface.execute(conn, String(x)) |> DataFrame

    return convert(Vector{Int}, ids.person_id)

end

"""
RaceFilterPersonIDs(race_codes, conn; tab::SQLTable = person)

Given a list of condition concept IDs, `race_codes`, return from the database individuals having at least one entry in the Person table matching at least one of the provided race types.

# Arguments:

- `race_codes` - a vector of `race_concept_id`'s; must be a subtype of `Integer`

`conn` - database connection using DBInterface

# Keyword Arguments:

- `tab::SQLTable` - the `SQLTable` representing the Person table; default `person`

# Returns

- `ids::Vector{Int64}` - the list of persons resulting from the filter
"""
@memoize Dict function RaceFilterPersonIDs(race_codes, conn; tab = person)
    ids =
        From(tab) |>
        Where(Fun.in(Get.race_concept_id, race_codes...)) |>
        Group(Get.person_id) |>
        q -> render(q, dialect = dialect) |>
        x -> DBInterface.execute(conn, String(x)) |> DataFrame

    return convert(Vector{Int}, ids.person_id)

end

"""
GenderFilterPersonIDs(gender_codes, conn; tab::SQLTable = visit_occurrence)

Given a list of visit concept IDs, `gender_codes` return from the database individuals having at least one entry in the Person table matching at least one of the provided gender types.

# Arguments:

- `visit_codes` - a vector of `gender_concept_id`'s; must be a subtype of `Integer`

`conn` - database connection using DBInterface

# Keyword Arguments:

- `tab::SQLTable` - the `SQLTable` representing the Person table; default `person`

# Returns

- `ids::Vector{Int64}` - the list of persons resulting from the filter
"""
@memoize Dict function GenderFilterPersonIDs(gender_codes, conn; tab = person)
    ids =
        From(tab) |>
        Where(Fun.in(Get.gender_concept_id, gender_codes...)) |>
        Group(Get.person_id) |>
        q -> render(q, dialect = dialect) |>
        x -> DBInterface.execute(conn, String(x)) |> DataFrame

    return convert(Vector{Int}, ids.person_id)

end

"""
StateFilterPersonIDs(states, conn; tab::SQLTable = location, join_tab::SQLTable = person)

Given a list of states, `states`, return from the database individuals found in the provided state list.

# Arguments:

- `states` - a vector of state abbreviations; must be a subtype of `AbstractString`

`conn` - database connection using DBInterface

# Keyword Arguments:

- `tab::SQLTable` - the `SQLTable` representing the Location table; default `location`
- `join_tab::SQLTable` - the `SQLTable` representing the Person table; default `person`

# Returns

- `ids::Vector{Int64}` - the list of persons resulting from the filter
"""
@memoize Dict function StateFilterPersonIDs(states, conn; tab::SQLTable = location, join_tab::SQLTable = person)
    ids =
        From(tab) |>
        Select(Get.location_id, Get.state) |>
        Where(Fun.in(Get.state, uppercase.(states)...)) |>
        Join(:join => join_tab, Get.location_id .== Get.join.location_id) |>
        Select(Get.join.person_id) |>
        q -> render(q, dialect = dialect) |>
        x -> DBInterface.execute(conn, String(x)) |> DataFrame

    return convert(Vector{Int}, ids.person_id)

end

export VisitFilterPersonIDs, ConditionFilterPersonIDs, RaceFilterPersonIDs, GenderFilterPersonIDs, StateFilterPersonIDs
