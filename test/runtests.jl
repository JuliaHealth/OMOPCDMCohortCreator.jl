using DataFrames
using Dates
using DBInterface
using FunSQL:
	From,
	Get,
	Group,
	Limit,
	Select,
	render
using HealthSampleData
using OMOPCDMCohortCreator
using SQLite
using Test
using TimeZones

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
	# @testset "SQLite Filter Functions" begin
	#	include("sqlite/filters.jl")
	# end
	# @testset "SQLite Generator Functions" begin
	#	include("sqlite/generators.jl")
	# end
	# @testset "SQLite Executors Functions" begin
	#	include("sqlite/generators.jl")
	# end

end


