"""
Internal function to determine the latest year for time-based calculations

# Arguments: 

`conn` - database connection using DBInterface

`calculated_year` - three different options can be used: 
    - `:now` - the year as of the day the code is executed given in UTC time
    - `:db` - the last year that any record was found in the database using the "observation_period" table (considered by OHDSI experts to have the latest records in a database)
    - any year provided by a user as long as it is an `Integer` (such as 2022, 1998, etc.)

# Returns

- `calculated_year` - the year that was determined to be used for further calculations
"""
function _determine_calculated_year(conn, calculated_year)
    if calculated_year == :now
        calculated_year = year(now(tz"UTC"))
    elseif calculated_year == :db
        #OPTIM: Get the max observation_period_end_date down to only one values
        #OPTIM: Figure out how to do this with FunSQL or default to a SQL string
        calculated_year = From(observation_period) |> Select(Get.observation_period_end_date) |> q -> render(q, dialect) |> x -> DBInterface.execute(conn, String(x)) |> DataFrame |> df -> maximum(df.observation_period_end_date) |> unix2datetime |> Dates.year
    end

    return calculated_year
end
