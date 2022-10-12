"""
GetDatabasePersonIDs(conn; tab::SQLTable = person)

Get all unique `person_id`'s from a database.

# Arguments:

`conn` - database connection using DBInterface

# Keyword Arguments:

- `tab::SQLTable` - the `SQLTable` representing the Person table; default `person`

# Returns

- `ids::Vector{Int64}` - the list of persons
"""
@memoize Dict function GetDatabasePersonIDs(conn; tab = person)
    ids =
        From(tab) |>
        Group(Get.person_id) |>
        q ->
            render(q, dialect = dialect) |>
            x -> DBInterface.execute(conn, String(x)) |> DataFrame

    return convert(Vector{Int}, ids.person_id)

end

"""
GetPatientState(ids, conn; tab::SQLTable = location, join_tab::SQLTable = person)

Given a list of person IDs, find their home state.

# Arguments:

`ids` - list of `person_id`'s; each ID must be of subtype `Integer`
`conn` - database connection using DBInterface

# Keyword Arguments:

- `tab::SQLTable` - the `SQLTable` representing the Location table; default `location`
- `join_tab::SQLTable` - the `SQLTable` representing the Person table; default `person`

# Returns

- `df::DataFrame` - a two column `DataFrame` comprised of columns: `:person_id` and `:state`
"""
@memoize Dict function GetPatientState(
    ids,
    conn;
    tab = location,
    join_tab = person,
)
    df =
        From(tab) |>
        Select(Get.location_id, Get.state) |>
        Join(:join => join_tab, Get.location_id .== Get.join.location_id) |>
        Where(Fun.in(Get.join.person_id, ids...)) |>
        Select(Get.join.person_id, Get.state) |>
        q ->
            render(q, dialect = dialect) |>
            x -> DBInterface.execute(conn, String(x)) |> DataFrame

    return df

end

"""
GetPatientGender(ids, conn; tab::SQLTable = person)

Given a list of person IDs, find their gender.

# Arguments:

`ids` - list of `person_id`'s; each ID must be of subtype `Integer`
`conn` - database connection using DBInterface

# Keyword Arguments:

- `tab::SQLTable` - the `SQLTable` representing the Person table; default `person`

# Returns

- `df::DataFrame` - a two column `DataFrame` comprised of columns: `:person_id` and `:gender_concept_id`
"""
@memoize Dict function GetPatientGender(
    ids,
    conn;
    tab = person,
)
    df =
        From(tab) |>
        Where(Fun.in(Get.person_id, ids...)) |>
        Select(Get.person_id, Get.gender_concept_id) |>
        q ->
            render(q, dialect = dialect) |>
            x -> DBInterface.execute(conn, String(x)) |> DataFrame

    return df

end

"""
GetPatientEthnicity(ids, conn; tab::SQLTable = person)

Given a list of person IDs, find their ethnicity.

# Arguments:

`ids` - list of `person_id`'s; each ID must be of subtype `Integer`
`conn` - database connection using DBInterface

# Keyword Arguments:

- `tab::SQLTable` - the `SQLTable` representing the Person table; default `person`

# Returns

- `df::DataFrame` - a two column `DataFrame` comprised of columns: `:person_id` and `:ethnicity_concept_id`
"""
@memoize Dict function GetPatientEthnicity(ids, conn; tab = person)
    df =
        From(tab) |>
        Where(Fun.in(Get.person_id, ids...)) |>
        Select(Get.person_id, Get.ethnicity_concept_id) |>
        q ->
            render(q, dialect = dialect) |>
            x -> DBInterface.execute(conn, String(x)) |> DataFrame

    return df

end

"""
GetPatientRace(ids, conn; tab::SQLTable = person)

Given a list of person IDs, find their race.

# Arguments:

`ids` - list of `person_id`'s; each ID must be of subtype `Integer`
`conn` - database connection using DBInterface

# Keyword Arguments:

- `tab::SQLTable` - the `SQLTable` representing the Person table; default `person`

# Returns

- `df::DataFrame` - a two column `DataFrame` comprised of columns: `:person_id` and `:race_concept_id`
"""
@memoize Dict function GetPatientRace(ids, conn; tab = person)
    df =
        From(tab) |>
        Where(Fun.in(Get.person_id, ids...)) |>
        Select(Get.person_id, Get.race_concept_id) |>
        q ->
            render(q, dialect = dialect) |>
            x -> DBInterface.execute(conn, String(x)) |> DataFrame

    return df

end

"""
GetPatientAgeGroup(
    ids, conn;
    minuend = :now,
    age_groupings = [
        [0, 9],
        [10, 19],
        [20, 29],
        [30, 39],
        [40, 49],
        [50, 59],
        [60, 69],
        [70, 79],
        [80, 89],
    ],
    tab::SQLTable = person,
)

Finds all individuals in age groups as specified by `age_groupings`.

# Arguments:

`ids` - list of `person_id`'s; each ID must be of subtype `Integer`
- `age_groupings` - a vector of age groups of the form `[[10, 19], [20, 29],]` denoting an age group of 10 - 19 and 20 - 29 respectively; age values must subtype of `Integer`

`conn` - database connection using DBInterface

# Keyword Arguments:

- `age_groupings` - a vector of age groups of the form `[[10, 19], [20, 29],]` denoting an age group of 10 - 19 and 20 - 29 respectively; age values must subtype of `Integer`

- `minuend` - the year that a patient's `year_of_birth` variable is subtracted from; default `:now`. There are three different options that can be set: 
    - `:now` - the year as of the day the code is executed given in UTC time
    - `:db` - the last year that any record was found in the database using the "observation_period" table (considered by OHDSI experts to have the latest records in a database)
    - any year provided by a user as long as it is an `Integer` (such as 2022, 1998, etc.)

- `tab::SQLTable` - the `SQLTable` representing the Person table; default `person`

# Returns

- `df::DataFrame` - a two column `DataFrame` comprised of columns: `:person_id` and `:age_group`

# Note

Age can be difficult to be calculated consistently.
In this case, there are some assumptions made to ensure consistency: 

1. According to the OMOP CDM v5.4, only the variable `year_of_birth` is guaranteed for a given patient. This is one of three options used as the minuend in age calculations.

2. The subtrahend is based on what one chooses for the `minuend` key word argument.

The age is then calculated following what is selected based on 1 and 2.
This flexibility is encoded to allow a user to choose how they want age groups calculated as well as clear up an ambiguity on how this is determined.
"""
@memoize Dict function GetPatientAgeGroup(
    ids,
    conn;
    minuend = :now,
    age_groupings = [
        [0, 9],
        [10, 19],
        [20, 29],
        [30, 39],
        [40, 49],
        [50, 59],
        [60, 69],
        [70, 79],
        [80, 89],
    ],
    tab::SQLTable = person
)
    minuend = _determine_calculated_year(conn, minuend)
    age_arr = []

    for grp in age_groupings
        push!(age_arr, Get.age .< grp[2] + 1)
        push!(age_arr, "$(grp[1]) - $(grp[2])")
    end

    From(tab) |>
    Where(Fun.in(Get.person_id, ids...)) |>
    Select(Get.person_id, :age => minuend .- Get.year_of_birth) |>
    Define(:age_group => Fun.case(age_arr...)) |>
    Select(Get.person_id, Get.age_group) |>
    q ->
        render(q, dialect = dialect) |>
        x -> DBInterface.execute(conn, String(x)) |> DataFrame

end

#TODO: Refactor patient visits to account for 
"""
GetPatientVisits(ids, conn; tab::SQLTable = visit_occurrence)

Given a list of person IDs, find all their visits.

# Arguments:

`ids` - list of `person_id`'s; each ID must be of subtype `Integer`
`conn` - database connection using DBInterface

# Keyword Arguments:

- `tab::SQLTable` - the `SQLTable` representing the Visit Occurrence table; default `visit_occurrence`

# Returns

- `df::DataFrame` - a two column `DataFrame` comprised of columns: `:person_id` and `:visit_occurrence_id`

"""
@memoize Dict function GetPatientVisits(
    ids,
    conn;
    tab::SQLTable = visit_occurrence,
)

    df =
        From(tab) |>
        Where(Fun.in(Get.person_id, ids...)) |>
        Select(Get.person_id, Get.visit_occurrence_id) |>
        q ->
            render(q, dialect = dialect) |>
            x -> DBInterface.execute(conn, String(x)) |> DataFrame

    return df

end

"""
GetMostRecentConditions(ids, conn; tab::SQLTable = condition_occurrence)

Given a list of person IDs, find their last recorded conditions.

# Arguments:

`ids` - list of `person_id`'s; each ID must be of subtype `Integer`
`conn` - database connection using DBInterface

# Keyword Arguments:

- `tab::SQLTable` - the `SQLTable` representing the Condition Occurrence table; default `condition_occurrence`

# Returns

- `df::DataFrame` - a two column `DataFrame` comprised of columns: `:person_id` and `:condition_concept_id`
"""
@memoize Dict function GetMostRecentConditions(
    ids,
    conn;
    tab::SQLTable = condition_occurrence,
)

    df =
        From(tab) |>
        Join(
            :date_tab =>
                From(tab) |>
                Where(Fun.in(Get.person_id, ids...)) |>
                Group(Get.person_id) |>
                Select(Get.person_id, :last_date => Agg.max(Get.condition_end_date)),
            on = Fun.and(
                Get.person_id .== Get.date_tab.person_id,
                Get.condition_end_date .== Fun.cast(Get.date_tab.last_date, "date"),
            ),
        ) |>
        Select(Get.person_id, Get.condition_concept_id) |>
        q ->
            render(q, dialect = dialect) |>
            x -> DBInterface.execute(conn, String(x)) |> DataFrame

    return df

end

"""
GetMostRecentVisit(ids, conn; tab::SQLTable = visit_occurrence)

Given a list of person IDs, find their last recorded visit.

# Arguments:

`ids` - list of `person_id`'s; each ID must be of subtype `Integer`
`conn` - database connection using DBInterface

# Keyword Arguments:

- `tab::SQLTable` - the `SQLTable` representing the Visit Occurrence table; default `visit_occurrence`

# Returns

- `df::DataFrame` - a two column `DataFrame` comprised of columns: `:person_id` and `:visit_occurrence_id`
"""
@memoize Dict function GetMostRecentVisit(
    ids,
    conn;
    tab::SQLTable = visit_occurrence,
)

    df =
        From(tab) |>
        Join(
            :date_tab =>
                From(tab) |>
                Where(Fun.in(Get.person_id, ids...)) |>
                Group(Get.person_id) |>
                Select(Get.person_id, :last_date => Agg.max(Get.visit_end_date)),
            on = Fun.and(
                Get.person_id .== Get.date_tab.person_id,
                Get.visit_end_date .== Fun.cast(Get.date_tab.last_date, "date"),
            ),
        ) |>
        Group(Get.person_id) |>
        Select(Get.person_id, :visit_occurrence_id => Agg.max(Get.visit_occurrence_id)) |> # ASSUMPTION: IF MULTIPLE VISITS IN ONE DAY, SELECT MOST RECENT visit_occurrence_id
        q ->
            render(q, dialect = dialect) |>
            x -> DBInterface.execute(conn, String(x)) |> DataFrame

    return df
end

"""
GetVisitCondition(visit_ids, conn; tab::SQLTable = visit_occurrence)

Given a list of visit IDs, find their corresponding conditions.

# Arguments:

`visit_ids` - list of `visit_id`'s; each ID must be of subtype `Integer`
`conn` - database connection using DBInterface

# Keyword Arguments:

- `tab::SQLTable` - the `SQLTable` representing the Condition Occurrence table; default `condition_occurrence`

# Returns

- `df::DataFrame` - a two column `DataFrame` comprised of columns: `:visit_occurrence_id` and `:condition_concept_id`
"""
@memoize Dict function GetVisitCondition(
    visit_ids,
    conn;
    tab::SQLTable = condition_occurrence,
)

    df =
        From(tab) |>
        Where(Fun.in(Get.visit_occurrence_id, visit_ids...)) |>
        Select(Get.visit_occurrence_id, Get.condition_concept_id) |>
        q ->
            render(q, dialect = dialect) |>
            x -> DBInterface.execute(conn, String(x)) |> DataFrame

    return df

end

export GetDatabasePersonIDs, GetPatientState, GetPatientGender, GetPatientRace, GetPatientAgeGroup, GetPatientVisits, GetMostRecentConditions, GetMostRecentVisit, GetVisitCondition
