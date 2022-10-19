"""
Internal function to determine the latest year for time-based calculations

# Arguments: 

`calculated_year` - three different options can be used: 
    - `:now` - the year as of the day the code is executed given in UTC time
    - any year provided by a user as long as it is an `Integer` (such as 2022, 1998, etc.)

# Returns

- `calculated_year` - the year that was determined to be used for further calculations
"""
function _determine_calculated_year(calculated_year)
    if calculated_year == :now
        calculated_year = year(now(tz"UTC"))
    end

    return calculated_year
end
