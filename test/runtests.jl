using DataFrames
using Dates
using DBInterface
using FunSQL:
	From,
	Fun,
	Get,
	Where,
	Group,
	Limit,
	Select,
	render, 
	Agg,
	LeftJoin
using HealthSampleData
using OMOPCDMCohortCreator
using SQLite
using Test
using TimeZones

# For allowing HealthSampleData to always download sample data
ENV["DATADEPS_ALWAYS_ACCEPT"] = true

# SQLite Data Source
sqlite_data = Eunomia()
sqlite_conn = SQLite.DB(sqlite_data)
GenerateDatabaseDetails(:sqlite, "main")
GenerateTables(sqlite_conn)

@testset "OMOPCDMCohortCreator" begin
	@testset "SQLite Helper Functions" begin
		include("sqlite/helpers.jl")
	end
	@testset "SQLite Getter Functions" begin
		include("sqlite/getters.jl")
	end
	@testset "SQLite Filter Functions" begin
		include("sqlite/filters.jl")
	end
	#= TODO: Add Generator function testset
	This set of tests needs a bit more scrutiny as there are some functions that need to be reviewed and most likely deprecated.
	labels: tests, moderate
	assignees: thecedarprince
	=#
	# @testset "SQLite Generator Functions" begin
	#	include("sqlite/generators.jl")
	# end
	@testset "SQLite Executors Functions" begin
		include("sqlite/executors.jl")
	end

end
