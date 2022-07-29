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
function GetDatabasePersonIDs(conn; tab = person)
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

# Keyword Arguments:

- `tab::SQLTable` - the `SQLTable` representing the Location table; default `location`
- `join_tab::SQLTable` - the `SQLTable` representing the Person table; default `person`

# Returns

- `df::DataFrame` - a two column `DataFrame` comprised of columns: `:person_id` and `:state`
"""
function GetPatientState(
    ids::Vector{T} where {T<:Integer},
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
          	GetPatientGender(ids; tab::SQLTable = person)

Given a list of person IDs, find their gender.

# Arguments:

`ids` - list of `person_id`'s; each ID must be of subtype `Integer`

# Keyword Arguments:

- `tab::SQLTable` - the `SQLTable` representing the Person table; default `person`

# Returns

- `df::DataFrame` - a two column `DataFrame` comprised of columns: `:person_id` and `:gender_concept_id`
"""
function GetPatientGender(
    ids::Vector{T} where {T<:Integer},
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
function GetPatientRace(ids::Vector{T} where {T<:Integer}, conn; tab = person)
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
    ids::Vector{T} where {T<:Integer}, conn;
    age_groupings::Vector{Vector{T}} where {T<:Integer} = [
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
    join_tab::SQLTable = observation_period,
)

Finds all individuals in age groups as specified by `age_groupings`.

# Arguments:

`ids` - list of `person_id`'s; each ID must be of subtype `Integer`
- `age_groupings` - a vector of age groups of the form `[[10, 19], [20, 29],]` denoting an age group of 10 - 19 and 20 - 29 respectively; age values must subtype of `Integer`
`conn` - database connection using DBInterface

# Keyword Arguments:

- `age_groupings` - a vector of age groups of the form `[[10, 19], [20, 29],]` denoting an age group of 10 - 19 and 20 - 29 respectively; age values must subtype of `Integer`
- `tab::SQLTable` - the `SQLTable` representing the Person table; default `person`
- `join_tab::SQLTable` - the `SQLTable` representing the Observation Period table; default `observation_period`

# Returns

- `df::DataFrame` - a two column `DataFrame` comprised of columns: `:person_id` and `:age_group`
"""
function GetPatientAgeGroup(
    ids::Vector{T} where {T<:Integer},
    conn;
    age_groupings::Vector{Vector{T}} where {T<:Integer} = [
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
    join_tab::SQLTable = observation_period,
)
    age_arr = []
    for grp in age_groupings
        push!(age_arr, Get.age .< grp[2] + 1)
        push!(age_arr, "$(grp[1]) - $(grp[2])")
    end

    From(tab) |>
    Where(Fun.in(Get.person_id, ids...)) |>
    LeftJoin(
        :observation_group => From(join_tab) |> Group(Get.person_id),
        on = Get.person_id .== Get.observation_group.person_id,
    ) |>
    Select(
        Get.person_id,
        Fun.make_date(Get.year_of_birth, Get.month_of_birth, Get.day_of_birth) |> As(:dob),
        Get.observation_group |> Agg.max(Get.observation_period_end_date) |> As(:record),
    ) |>
    Select(Get.person_id, :age => Fun.date_part("year", Fun.age(Get.record, Get.dob))) |>
    Define(:age_group => Fun.case(age_arr...)) |>
    Select(Get.person_id, Get.age_group) |>
    q ->
        render(q, dialect = dialect) |>
        x -> DBInterface.execute(conn, String(x)) |> DataFrame

end

"""
TODO: Add documentation later
"""
function GetPatientVisits(
    ids::Vector{T} where {T<:Integer},
    conn;
    tab::SQLTable = visit_occurrence,
)
    df =
        From(tab) |>
        Where(Fun.in(Get.person_id, ids...)) |>
        Select(Get.person_id, Get.visit_concept_id) |>
        q ->
            render(q, dialect = dialect) |>
            x -> DBInterface.execute(conn, String(x)) |> DataFrame

    return df

end

"""
          	GetMostRecentConditions(ids::Vector{T} where {T<:Integer}, conn; tab::SQLTable = condition_occurrence)

Given a list of person IDs, find their last recorded conditions.

# Arguments:

`ids` - list of `person_id`'s; each ID must be of subtype `Integer`

`conn` - database connection using DBInterface

# Keyword Arguments:

- `tab::SQLTable` - the `SQLTable` representing the Condition Occurrence table; default `condition_occurrence`

# Returns

- `df::DataFrame` - a two column `DataFrame` comprised of columns: `:person_id` and `:condition_concept_id`
"""
function GetMostRecentConditions(
    ids::Vector{T} where {T<:Integer},
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
          	GetMostRecentVisit(ids::Vector{T} where {T<:Integer}, conn; tab::SQLTable = visit_occurrence)

Given a list of person IDs, find their last recorded visit.

# Arguments:

`ids` - list of `person_id`'s; each ID must be of subtype `Integer`

`conn` - database connection using DBInterface

# Keyword Arguments:

- `tab::SQLTable` - the `SQLTable` representing the Visit Occurrence table; default `visit_occurrence`

# Returns

- `df::DataFrame` - a two column `DataFrame` comprised of columns: `:person_id` and `:visit_occurrence_id`
"""
function GetMostRecentVisit(
    ids::Vector{T} where {T<:Integer},
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
          	GetVisitCondition(visit_ids::Vector{T} where {T<:Integer}, conn; tab::SQLTable = visit_occurrence)

Given a list of visit IDs, find their corresponding conditions.

# Arguments:

`visit_ids` - list of `visit_id`'s; each ID must be of subtype `Integer`

`conn` - database connection using DBInterface

# Keyword Arguments:

- `tab::SQLTable` - the `SQLTable` representing the Condition Occurrence table; default `condition_occurrence`

# Returns

- `df::DataFrame` - a two column `DataFrame` comprised of columns: `:visit_occurrence_id` and `:condition_concept_id`
"""
function GetVisitCondition(
    visit_ids::Vector{T} where {T<:Integer},
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

