@testset "Date Functions" begin
	year_now = year(now(tz"UTC"))
	eunomia_latest_year = 2019

	@test year_now == OMOPCDMCohortCreator._determine_calculated_year(sqlite_conn, :now)
	@test eunomia_latest_year == OMOPCDMCohortCreator._determine_calculated_year(sqlite_conn, :db)
	@test 2022 == OMOPCDMCohortCreator._determine_calculated_year(sqlite_conn, 2022)
end
