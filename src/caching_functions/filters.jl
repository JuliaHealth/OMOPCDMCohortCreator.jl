"""
VisitFilterPersonIDs(visit_codes; tab::SQLTable = visit_occurrence)

Given a list of visit concept IDs, `visit_codes`  return from the database individuals having at least one entry in the Visit Occurrence table matching at least one of the provided visit types.

# Arguments:

- `visit_codes` - a vector of `visit_concept_id`'s; must be a subtype of `Integer`

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
ConditionFilterPersonIDs(condition_codes; tab::SQLTable = condition_occurrence)

Given a list of condition concept IDs, `condition_codes`, return from the database individuals having at least one entry in the Condition Occurrence table matching at least one of the provided condition types.

# Arguments:

- `condition_codes` - a vector of `condition_concept_id`'s; must be a subtype of `Integer`

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
RaceFilterPersonIDs(race_codes; tab::SQLTable = person)

Given a list of condition concept IDs, `race_codes`, return from the database individuals having at least one entry in the Person table matching at least one of the provided race types.

# Arguments:

- `race_codes` - a vector of `race_concept_id`'s; must be a subtype of `Integer`

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
GenderFilterPersonIDs(gender_codes; tab::SQLTable = visit_occurrence)

Given a list of visit concept IDs, `gender_codes` return from the database individuals having at least one entry in the Person table matching at least one of the provided gender types.

# Arguments:

- `visit_codes` - a vector of `gender_concept_id`'s; must be a subtype of `Integer`

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
StateFilterPersonIDs(states; tab::SQLTable = location, join_tab::SQLTable = person)

Given a list of states, `states`, return from the database individuals found in the provided state list.

# Arguments:

- `states` - a vector of state abbreviations; must be a subtype of `AbstractString`

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

"""
AgeGroupFilterPersonIDs(age_groupings; tab::SQLTable = location, join_tab::SQLTable = observation_period)

Finds all individuals in age groups as specified by `age_groupings`.

# Arguments:

- `age_groupings` - a vector of age groups of the form `[[10, 19], [20, 29],]` denoting an age group of 10 - 19 and 20 - 29 respectively; age values must subtype of `Integer`

# Keyword Arguments:

- `tab::SQLTable` - the `SQLTable` representing the Person table; default `person`
- `join_tab::SQLTable` - the `SQLTable` representing the Observation Period table; default `observation_period`

# Returns

- `ids::Vector{Int64}` - the list of persons resulting from the filter
"""
@memoize Dict function AgeGroupFilterPersonIDs(age_groupings, conn; tab = person, join_tab = observation_period)

    age_arr = []
    age_ranges = []
    for grp in age_groupings
        push!(age_arr, Get.age .< grp[2] + 1)
        push!(age_arr, "$(grp[1]) - $(grp[2])")
        push!(age_ranges, "$(grp[1]) - $(grp[2])")
    end

    ids =
        From(person) |>
        LeftJoin(
            :observation_group => From(join_tab) |> Group(Get.person_id),
            on = Get.person_id .== Get.observation_group.person_id,
        ) |>
        Select(
            Get.person_id,
            Fun.make_date(Get.year_of_birth, Get.month_of_birth, Get.day_of_birth) |>
            As(:dob), #BUG: This only works for PostgreSQL, MySQL right now; breaks on SQLite.
            #TODO: Refactor how to get the date agnostic from implementation: https://www.sqltutorial.org/sql-date-functions/sql-convert-string-to-date-functions/
            Get.observation_group |>
            Agg.max(Get.observation_period_end_date) |>
            As(:record),
        ) |>
        Select(
            Get.person_id,
            :age => Fun.date_part("year", Fun.age(Get.record, Get.dob)),
        ) |>
        Define(:age_group => Fun.case(age_arr...)) |>
        Where(Fun.in(Get.age_group, age_ranges...)) |>
        Select(Get.person_id) |>
        q -> render(q, dialect = dialect) |>
        x -> DBInterface.execute(conn, String(x)) |> DataFrame

    return convert(Vector{Int}, ids.person_id)

end

export VisitFilterPersonIDs, ConditionFilterPersonIDs, RaceFilterPersonIDs, GenderFilterPersonIDs, StateFilterPersonIDs, AgeGroupFilterPersonIDs
