module OMOPCDMCohortCreator

#TODO: Bump minor version due to new dependencies added

using DataFrames
using Dates
using DBInterface
using FunSQL:
    SQLTable,
    Agg,
    As,
    Define,
    From,
    Fun,
    Get,
    Group,
    Join,
    Order,
    Select,
    WithExternal,
    Where,
    render,
    Limit,
    ID,
    LeftJoin,
    reflect
using Memoization
using TimeZones

include("caching_functions/getters.jl")
include("caching_functions/filters.jl")
include("generators.jl")
include("executors.jl")

end
