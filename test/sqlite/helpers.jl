@testset "Date Functions" begin
	year_now = year(now(tz"UTC"))

	@test year_now == OMOPCDMCohortCreator._determine_calculated_year(:now)
	@test 2022 == OMOPCDMCohortCreator._determine_calculated_year(2022)
end
