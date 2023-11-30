using DataFrames
using Dates
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

using JSON3
using OHDSICohortExpressions: translate, Model

import DBInterface as DBI

# For allowing HealthSampleData to always download sample data
ENV["DATADEPS_ALWAYS_ACCEPT"] = true

# SQLite Data Source
sqlite_conn = SQLite.DB(Eunomia())
GenerateDatabaseDetails(:sqlite, "main")
GenerateTables(sqlite_conn)

cohort = read("./assets/strep_throat.json", String)

#using DBInterface

model = Model(cdm_version=v"5.3.1", cdm_schema="main",
                     vocabulary_schema="main", results_schema="main",
                     target_schema="main", target_table="cohort");

sql = translate(cohort, dialect=:sqlite, model=model,
                         cohort_definition_id=1);

[DBI.execute(sqlite_conn, sub_query) for sub_query in split(sql, ";")[1:end-1]]

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
