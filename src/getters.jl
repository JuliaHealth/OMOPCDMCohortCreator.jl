"""
GetDatabasePersonIDs(conn; tab = person)

Get all unique `person_id`'s from a database.

# Arguments:

- `conn` - database connection using DBInterface

# Keyword Arguments:

- `tab` - the `SQLTable` representing the Person table; default `person`

# Returns

- `ids::Vector{Int64}` - the list of persons
"""
function GetDatabasePersonIDs(conn; tab=person)
    ids = DBInterface.execute(conn, String(GetDatabasePersonIDs(tab=tab))) |> DataFrame

    return convert(Vector{Int}, ids.person_id)

end

"""
GetDatabaseYearRange(conn; tab = observation_period)

Get the years for which data is available from a database.

# Arguments:

- `conn` - database connection using DBInterface

# Keyword Arguments:

- `tab` - the `SQLTable` representing the Observation Period table; default `observation_period`

# Returns

- `year_range::NamedTuple{(:first_year, :last_year), Tuple{Int64, Int64}}` - a NamedTuple where `first_year` is the first year data from the database was available and `last_year` where the last year data from the database was available
"""
function GetDatabaseYearRange(conn; tab=observation_period)
    years = DBInterface.execute(conn, String(GetDatabaseYearRange(tab=tab))) |> DataFrame

    return (first_year=first(years.first_year), last_year=first(years.last_year))

end

"""
GetDatabaseYearRange(; tab = observation_period)

Return SQL to find the years for which data is available from a database.

# Keyword Arguments:

- `tab` - the `SQLTable` representing the Observation Period table; default `observation_period`

# Returns

- `year_range::NamedTuple{(:first_year, :last_year), Tuple{Int64, Int64}}` - a NamedTuple where `first_year` is the first year data from the database was available and `last_year` where the last year data from the database was available
"""
function GetDatabaseYearRange(; tab=observation_period)
    sql = From(tab) |>
          Group() |>
          Select(:first_year => Agg.min(Get.observation_period_end_date),
              :last_year => Agg.max(Get.observation_period_end_date)) |>
          q -> render(q, dialect=OMOPCDMCohortCreator.dialect)

    return String(sql)

end

"""
GetDatabasePersonIDs(; tab = person)

Return SQL statement that gets all unique `person_id`'s from a database.

# Keyword Arguments:

- `tab` - the `SQLTable` representing the Person table; default `person`

# Returns

- `sql::String` - Prepared SQL statement as a `String`
"""
function GetDatabasePersonIDs(; tab=person)
    sql =
        From(tab) |>
        Group(Get.person_id) |>
        q ->
            render(q, dialect=dialect)

    return String(sql)
end

"""
GetPatientState(ids, conn; tab = location, join_tab = person)

Given a list of person IDs, find their home state.

# Arguments:

- `ids` - list of `person_id`'s; each ID must be of subtype `Integer`

- `conn` - database connection using DBInterface

# Keyword Arguments:

- `tab` - the `SQLTable` representing the Location table; default `location`

- `join_tab` - the `SQLTable` representing the Person table; default `person`

# Returns

- `df::DataFrame` - a two column `DataFrame` comprised of columns: `:person_id` and `:state`
"""
function GetPatientState(
    ids,
    conn;
    tab=location,
    join_tab=person
)
    df = DBInterface.execute(conn, GetPatientState(ids; tab=tab, join_tab=join_tab)) |> DataFrame

    return df

end

"""
GetPatientState(ids; tab = location, join_tab = person)

Return SQL statement where if given a list of person IDs, find their home state.

# Arguments:

- `ids` - list of `person_id`'s; each ID must be of subtype `Integer`

- `conn` - database connection using DBInterface

# Keyword Arguments:

- `tab` - the `SQLTable` representing the Location table; default `location`

- `join_tab` - the `SQLTable` representing the Person table; default `person`

# Returns

- `df::DataFrame` - a two column `DataFrame` comprised of columns: `:person_id` and `:state`
"""
function GetPatientState(
    ids;
    tab=location,
    join_tab=person
)
    sql =
        From(tab) |>
        Select(Get.location_id, Get.state) |>
        Join(:join => join_tab, Get.location_id .== Get.join.location_id) |>
        Where(Fun.in(Get.join.person_id, ids...)) |>
        Select(Get.join.person_id, Get.state) |>
        q -> render(q, dialect=dialect)

    return String(sql)

end

"""
GetPatientGender(ids, conn; tab = person)

Given a list of person IDs, find their gender.

# Arguments:

- `ids` - list of `person_id`'s; each ID must be of subtype `Integer`

- `conn` - database connection using DBInterface

# Keyword Arguments:

- `tab` - the `SQLTable` representing the Person table; default `person`

# Returns

- `df::DataFrame` - a two column `DataFrame` comprised of columns: `:person_id` and `:gender_concept_id`
"""
function GetPatientGender(
    ids,
    conn;
    tab=person
)
    df = DBInterface.execute(conn, GetPatientGender(ids; tab=tab)) |> DataFrame

    return df

end

"""
GetPatientGender(ids; tab = person)

Return SQL statement that gets the `gender_concept_id` for a given list of `person_id`'s

# Arguments:

- `ids` - list of `person_id`'s; each ID must be of subtype `Integer`

# Keyword Arguments:

- `tab` - the `SQLTable` representing the Person table; default `person`

# Returns

- `df::DataFrame` - a two column `DataFrame` comprised of columns: `:person_id` and `:gender_concept_id`
"""
function GetPatientGender(
    ids;
    tab=person
)
    sql =
        From(tab) |>
        Where(Fun.in(Get.person_id, ids...)) |>
        Select(Get.person_id, Get.gender_concept_id) |>
        q -> render(q, dialect=dialect)

    return String(sql)

end

"""
GetPatientEthnicity(ids, conn; tab = person)

Given a list of person IDs, find their ethnicity.

# Arguments:

- `ids` - list of `person_id`'s; each ID must be of subtype `Integer`

- `conn` - database connection using DBInterface

# Keyword Arguments:

- `tab` - the `SQLTable` representing the Person table; default `person`

# Returns

- `df::DataFrame` - a two column `DataFrame` comprised of columns: `:person_id` and `:ethnicity_concept_id`
"""
function GetPatientEthnicity(
    ids,
    conn;
    tab=person
)
    df = DBInterface.execute(conn, GetPatientEthnicity(ids; tab=tab)) |> DataFrame

    return df

end

"""
GetPatientEthnicity(ids, conn; tab = person)

Return SQL statement that gets the `ethnicity_concept_id` for a given list of `person_id`'s

# Arguments:

- `ids` - list of `person_id`'s; each ID must be of subtype `Integer`

# Keyword Arguments:

- `tab` - the `SQLTable` representing the Person table; default `person`

# Returns

- `df::DataFrame` - a two column `DataFrame` comprised of columns: `:person_id` and `:ethnicity_concept_id`
"""
function GetPatientEthnicity(ids; tab=person)
    sql =
        From(tab) |>
        Where(Fun.in(Get.person_id, ids...)) |>
        Select(Get.person_id, Get.ethnicity_concept_id) |>
        q ->
            render(q, dialect=dialect)

    return String(sql)

end

"""
GetPatientRace(ids, conn; tab = person)

Given a list of person IDs, find their race.

# Arguments:

- `ids` - list of `person_id`'s; each ID must be of subtype `Integer`

- `conn` - database connection using DBInterface

# Keyword Arguments:

- `tab` - the `SQLTable` representing the Person table; default `person`

# Returns

- `df::DataFrame` - a two column `DataFrame` comprised of columns: `:person_id` and `:race_concept_id`
"""
function GetPatientRace(ids, conn; tab=person)
    df = DBInterface.execute(conn, GetPatientRace(ids; tab=tab)) |> DataFrame

    return df

end

"""
GetPatientRace(ids; tab = person)

Return SQL statement that gets the `race_concept_id` for a given list of `person_id`'s

# Arguments:

- `ids` - list of `person_id`'s; each ID must be of subtype `Integer`

# Keyword Arguments:

- `tab` - the `SQLTable` representing the Person table; default `person`

# Returns

- `df::DataFrame` - a two column `DataFrame` comprised of columns: `:person_id` and `:race_concept_id`
"""
function GetPatientRace(ids; tab=person)
    sql =
        From(tab) |>
        Where(Fun.in(Get.person_id, ids...)) |>
        Select(Get.person_id, Get.race_concept_id) |>
        q -> render(q, dialect=dialect)

    return String(sql)

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
    tab = person,
)

Finds all individuals in age groups as specified by `age_groupings`.

# Arguments:

- `ids` - list of `person_id`'s; each ID must be of subtype `Integer`

- `conn` - database connection using DBInterface

# Keyword Arguments:

- `age_groupings` - a vector of age groups of the form `[[10, 19], [20, 29],]` denoting an age group of 10 - 19 and 20 - 29 respectively; age values must subtype of `Integer`

- `minuend` - the year that a patient's `year_of_birth` variable is subtracted from; default `:now`. There are two different options that can be set: 
    - `:now` - the year as of the day the code is executed given in UTC time
    - any year provided by a user as long as it is an `Integer` (such as 2022, 1998, etc.)

- `tab` - the `SQLTable` representing the Person table; default `person`

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
function GetPatientAgeGroup(
    ids,
    conn;
    minuend=:now,
    age_groupings=[
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
    tab=person
)

    df = DBInterface.execute(conn, GetPatientAgeGroup(ids; minuend=minuend, age_groupings=age_groupings, tab=tab)) |> DataFrame

    return df

end

"""
GetPatientAgeGroup(
    ids;
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
    tab = person,
)

Return SQL statement that assigns an age group to each patient in a given patient list. 
Customized age groupings can be provided as a list.

# Arguments:

- `ids` - list of `person_id`'s; each ID must be of subtype `Integer`

# Keyword Arguments:

- `age_groupings` - a vector of age groups of the form `[[10, 19], [20, 29],]` denoting an age group of 10 - 19 and 20 - 29 respectively; age values must subtype of `Integer`

- `minuend` - the year that a patient's `year_of_birth` variable is subtracted from; default `:now`. There are two different options that can be set: 
    - `:now` - the year as of the day the code is executed given in UTC time
    - any year provided by a user as long as it is an `Integer` (such as 2022, 1998, etc.)

- `tab` - the `SQLTable` representing the Person table; default `person`

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
function GetPatientAgeGroup(
    ids;
    minuend=:now,
    age_groupings=[
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
    tab=person
)

    minuend = _determine_calculated_year(minuend)
    age_arr = []

    for grp in age_groupings
        push!(age_arr, Get.age .< grp[2] + 1)
        push!(age_arr, "$(grp[1]) - $(grp[2])")
    end

    sql = From(tab) |>
          Where(Fun.in(Get.person_id, ids...)) |>
          Select(Get.person_id, :age => minuend .- Get.year_of_birth) |>
          Define(:age_group => Fun.case(age_arr...)) |>
          Select(Get.person_id, Get.age_group) |>
          q -> render(q, dialect=dialect)

    return String(sql)

end

"""
GetPatientVisits(ids, conn; tab = visit_occurrence)

Given a list of person IDs, find all their visits.

# Arguments:

- `ids` - list of `person_id`'s; each ID must be of subtype `Integer`

- `conn` - database connection using DBInterface

# Keyword Arguments:

- `tab` - the `SQLTable` representing the Visit Occurrence table; default `visit_occurrence`

# Returns

- `df::DataFrame` - a two column `DataFrame` comprised of columns: `:person_id` and `:visit_occurrence_id`

"""
function GetPatientVisits(
    ids,
    conn;
    tab=visit_occurrence
)

    df = DBInterface.execute(conn, GetPatientVisits(ids; tab=tab)) |> DataFrame

    return df

end

"""
GetPatientVisits(ids; tab = visit_occurrence)

Return SQL statement that returns all `visit_occurrence_id` for a given patient list

# Arguments:

- `ids` - list of `person_id`'s; each ID must be of subtype `Integer`

# Keyword Arguments:

- `tab` - the `SQLTable` representing the Visit Occurrence table; default `visit_occurrence`

# Returns

- `df::DataFrame` - a two column `DataFrame` comprised of columns: `:person_id` and `:visit_occurrence_id`

"""
function GetPatientVisits(
    ids;
    tab=visit_occurrence
)

    sql =
        From(tab) |>
        Where(Fun.in(Get.person_id, ids...)) |>
        Select(Get.person_id, Get.visit_occurrence_id) |>
        q -> render(q, dialect=dialect)

    return String(sql)

end

"""
GetMostRecentConditions(ids, conn; tab = condition_occurrence)

Given a list of person IDs, find their last recorded conditions.

# Arguments:

- `ids` - list of `person_id`'s; each ID must be of subtype `Integer`

- `conn` - database connection using DBInterface

# Keyword Arguments:

- `tab` - the `SQLTable` representing the Condition Occurrence table; default `condition_occurrence`

# Returns

- `df::DataFrame` - a two column `DataFrame` comprised of columns: `:person_id` and `:condition_concept_id`
"""
function GetMostRecentConditions(
    ids,
    conn;
    tab=condition_occurrence
)

    df = DBInterface.execute(conn, GetMostRecentConditions(ids; tab=tab)) |> DataFrame

    return df

end

"""
GetMostRecentConditions(ids; tab = condition_occurrence)

Produces SQL statement that, given a list of person IDs, finds their last recorded conditions.

# Arguments:

- `ids` - list of `person_id`'s; each ID must be of subtype `Integer`

# Keyword Arguments:

- `tab` - the `SQLTable` representing the Condition Occurrence table; default `condition_occurrence`

# Returns

- `df::DataFrame` - a two column `DataFrame` comprised of columns: `:person_id` and `:condition_concept_id`
"""
function GetMostRecentConditions(
    ids;
    tab=condition_occurrence
)

    sql =
        From(tab) |>
        Join(
            :date_tab =>
                From(tab) |>
                Where(Fun.in(Get.person_id, ids...)) |>
                Group(Get.person_id) |>
                Select(Get.person_id, :last_date => Agg.max(Get.condition_end_date)),
            on=Fun.and(
                Get.person_id .== Get.date_tab.person_id,
                Get.condition_end_date .== Get.date_tab.last_date,
            ),
        ) |>
        Select(Get.person_id, Get.condition_concept_id) |>
        q -> render(q, dialect=dialect)

    return String(sql)

end

"""
GetMostRecentVisit(ids, conn; tab = visit_occurrence)

Given a list of person IDs, find their last recorded visit.

# Arguments:

- `ids` - list of `person_id`'s; each ID must be of subtype `Integer`

- `conn` - database connection using DBInterface

# Keyword Arguments:

- `tab` - the `SQLTable` representing the Visit Occurrence table; default `visit_occurrence`

# Returns

- `df::DataFrame` - a two column `DataFrame` comprised of columns: `:person_id` and `:visit_occurrence_id`
"""
function GetMostRecentVisit(
    ids,
    conn;
    tab=visit_occurrence
)

    df = DBInterface.execute(conn, GetMostRecentVisit(ids; tab=tab)) |> DataFrame

    return df
end

"""
GetMostRecentVisit(ids, conn; tab = visit_occurrence)

Produces SQL statement that, given a list of person IDs, finds their last recorded visit.

# Arguments:

- `ids` - list of `person_id`'s; each ID must be of subtype `Integer`

# Keyword Arguments:

- `tab` - the `SQLTable` representing the Visit Occurrence table; default `visit_occurrence`

# Returns

- `df::DataFrame` - a two column `DataFrame` comprised of columns: `:person_id` and `:visit_occurrence_id`
"""
function GetMostRecentVisit(
    ids;
    tab=visit_occurrence
)

    sql =
        From(tab) |>
        Join(
            :date_tab =>
                From(tab) |>
                Where(Fun.in(Get.person_id, ids...)) |>
                Group(Get.person_id) |>
                Select(Get.person_id, :last_date => Agg.max(Get.visit_end_date)),
            on=Fun.and(
                Get.person_id .== Get.date_tab.person_id,
                Get.visit_end_date .== Fun.cast(Get.date_tab.last_date, "date"),
            ),
        ) |>
        Group(Get.person_id) |>
        Select(Get.person_id, :visit_occurrence_id => Agg.max(Get.visit_occurrence_id)) |> # ASSUMPTION: IF MULTIPLE VISITS IN ONE DAY, SELECT MOST RECENT visit_occurrence_id
        q -> render(q, dialect=dialect)

    return String(sql)
end

"""
GetVisitCondition(visit_ids, conn; tab = visit_occurrence)

Given a list of visit IDs, find their corresponding conditions.

# Arguments:

- `visit_ids` - list of `visit_id`'s; each ID must be of subtype `Integer`

- `conn` - database connection using DBInterface

# Keyword Arguments:

- `tab` - the `SQLTable` representing the Condition Occurrence table; default `condition_occurrence`

# Returns

- `df::DataFrame` - a two column `DataFrame` comprised of columns: `:visit_occurrence_id` and `:condition_concept_id`
"""
function GetVisitCondition(
    visit_ids,
    conn;
    tab=condition_occurrence
)

    df = DBInterface.execute(conn, GetVisitCondition(visit_ids; tab=tab)) |> DataFrame

    return df

end

"""
GetVisitDate(visit_occurrence_id; interval::Symbol = :start, tab = visit_occurrence)

This function queries a database for the start or end date of the visit occurrence associated with the given `visit_occurrence_id` or list of `visit_occurrence_id`'s.

# Arguments:
- `visit_occurrence_id`: A single `visit_occurrence_id` or a vector of `visit_occurrence_id`'s to query for.
- `interval`: A keyword argument that determines whether to query for the visit start date (`interval=:start`) or the visit end date (`interval=:end`). Default value is `interval=:start`.

# Returns:
A dataframe with two columns: `visit_occurrence_id` and either `visit_start_date` or `visit_end_date`, depending on the value of the `interval` argument.
"""
function GetVisitCondition(
    visit_ids;
    tab=condition_occurrence
)

    sql =
        From(tab) |>
        Where(Fun.in(Get.visit_occurrence_id, visit_ids...)) |>
        Select(Get.visit_occurrence_id, Get.condition_concept_id) |>
        q -> render(q, dialect=dialect)

    return String(sql)

end

#= TODO: Write tests for GetVisitPlaceOfService
Only needs one or two tests; may be difficult to test as I do not think Eunomia has anything other than missing
labels: tests, good first issue
assignees: VarshC
=#

"""
GetVisitPlaceOfService(visit_ids, conn; tab = visit_occurrence, join_tab = care_site)

Given a list of visit IDs, find their place of service 

# Arguments:

- `visit_ids` - list of `visit_id`'s; each ID must be of subtype `Integer`

- `conn` - database connection using DBInterface

# Keyword Arguments:

- `tab` - the `SQLTable` representing the Condition Occurrence table; default `visit_occurrence`

- `join_tab` - the `SQLTable` representing the Person table; default `care_site`

# Returns

- `df::DataFrame` - a two column `DataFrame` comprised of columns: `:visit_occurrence_id` and `:condition_concept_id`
"""
function GetVisitPlaceOfService(
    visit_ids,
    conn;
    tab=visit_occurrence,
    join_tab=care_site
)

    df = DBInterface.execute(conn, GetVisitPlaceOfService(visit_ids; tab=tab, join_tab=join_tab)) |> DataFrame

    return df

end

"""
GetVisitPlaceOfService(visit_ids; tab = visit_occurrence, join_tab = care_site)

Produces SQL statement that, given a list of visit IDs, find their place of service 

# Arguments:

- `visit_ids` - list of `visit_id`'s; each ID must be of subtype `Integer`

# Keyword Arguments:

- `tab` - the `SQLTable` representing the Condition Occurrence table; default `visit_occurrence`

- `join_tab` - the `SQLTable` representing the Person table; default `care_site`

# Returns

- `sql::String` - Prepared SQL statement as a `String`
"""
function GetVisitPlaceOfService(
    visit_ids;
    tab=visit_occurrence,
    join_tab=care_site
)

    sql =
        From(tab) |>
        Where(Fun.in(Get.visit_occurrence_id, visit_ids...)) |>
        Select(Get.visit_occurrence_id, Get.care_site_id) |>
        Join(:join => join_tab, Get.care_site_id .== Get.join.care_site_id) |>
        Select(Get.visit_occurrence_id, Get.join.place_of_service_concept_id) |> 
        q -> render(q, dialect=dialect)

    return String(sql)

end

#= TODO: Write tests for GetVisitConcept
Only needs one or two tests; should have everything that is required in Eunomia to run!
labels: tests, good first issue
assignees: VarshC
=#
"""
GetVisitConcept(visit_ids, conn; tab = visit_occurrence)

Given a list of visit IDs, find their corresponding visit_concept_id's.

# Arguments:

- `visit_ids` - list of `visit_id`'s; each ID must be of subtype `Integer`

- `conn` - database connection using DBInterface

# Keyword Arguments:

- `tab` - the `SQLTable` representing the Condition Occurrence table; default `visit_occurrence`

# Returns

- `df::DataFrame` - a two column `DataFrame` comprised of columns: `:visit_occurrence_id` and `:visit_concept_id`
"""
function GetVisitConcept(
    visit_ids,
    conn;
    tab=visit_occurrence
)

    df = DBInterface.execute(conn, GetVisitConcept(visit_ids; tab=tab)) |> DataFrame

    return df

end

"""
GetVisitConcept(visit_ids; tab = visit_occurrence)

Produces SQL statement that, given a list of visit IDs, find their corresponding visit_concept_id's.

# Arguments:

- `visit_ids` - list of `visit_id`'s; each ID must be of subtype `Integer`

# Keyword Arguments:

- `tab` - the `SQLTable` representing the Condition Occurrence table; default `visit_occurrence`

# Returns

- `sql::String` - Prepared SQL statement as a `String`
"""
function GetVisitConcept(
    visit_ids;
    tab=visit_occurrence
)

    sql =
        From(tab) |>
        Where(Fun.in(Get.visit_occurrence_id, visit_ids...)) |>
        Select(Get.visit_occurrence_id, Get.visit_concept_id) |>
        q -> render(q, dialect=dialect)

    return String(sql)

end

"""
GetVisitDate(visit_occurrence_id; interval::Symbol = :start, tab = visit_occurrence)

This function queries a database for the start or end date of the visit occurrence associated with the given `visit_occurrence_id` or list of `visit_occurrence_id`'s.

# Arguments:
- `visit_occurrence_id`: A single `visit_occurrence_id` or a vector of `visit_occurrence_id`'s to query for.
- `interval`: A keyword argument that determines whether to query for the visit start date (`interval=:start`) or the visit end date (`interval=:end`). Default value is `interval=:start`.

# Returns:
A dataframe with two columns: `visit_occurrence_id` and either `visit_start_date` or `visit_end_date`, depending on the value of the `interval` argument.
"""

function GetVisitDate(
    visit_occurrence_ids,
    conn;
    interval::Symbol = :start,
    tab=visit_occurrence
)
    df = DBInterface.execute(conn, GetVisitDate(visit_occurrence_ids; interval, tab=tab)) |> DataFrame
    return df
end



function GetVisitDate(
    visit_occurrence_id;
    interval::Symbol = :start,
    tab=visit_occurrence
)
    if (interval == :start)
        sql =
            From(tab) |>
            Where(Fun.in(Get.visit_occurrence_id, visit_occurrence_id...)) |>
            Select(Get.visit_occurrence_id, Get.visit_start_date) |>
            q -> render(q, dialect=dialect)
        return String(sql)
    elseif (interval == :end)
        sql =
        From(tab) |>
        Where(Fun.in(Get.visit_occurrence_id, visit_occurrence_id...)) |>
        Select(Get.visit_occurrence_id, Get.visit_end_date) |>
        q -> render(q, dialect=dialect)
        return String(sql)
    else
        return "NA"
    end

end


export GetDatabasePersonIDs, GetPatientState, GetPatientGender, GetPatientRace, GetPatientAgeGroup, GetPatientVisits, GetMostRecentConditions, GetMostRecentVisit, GetVisitCondition, GetPatientEthnicity, GetDatabaseYearRange, GetVisitPlaceOfService, GetVisitConcept, GetVisitDate
