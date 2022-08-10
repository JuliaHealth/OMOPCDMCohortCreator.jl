@testset "GetDatabasePersonIDs Tests" begin
	ids = From(OMOPCDMCohortCreator.person) |> Select(Get.person_id) |> q -> render(q, dialect = OMOPCDMCohortCreator.dialect) |> q -> DBInterface.execute(sqlite_conn, q) |> DataFrame |> x -> convert(Vector{Int}, x.person_id)

	@test ids == GetDatabasePersonIDs(sqlite_conn)
end

# @testset "GetPatientState Tests" begin
# 
# end

@testset "GetPatientGender Tests" begin
	genders = From(OMOPCDMCohortCreator.person) |> Select(Get.person_id, Get.gender_concept_id) |> Limit(20) |> q -> render(q, dialect = OMOPCDMCohortCreator.dialect) |> q -> DBInterface.execute(sqlite_conn, q) |> DataFrame

	test_ids = From(OMOPCDMCohortCreator.person) |> Select(Get.person_id) |> Limit(20) |> q -> render(q, dialect = OMOPCDMCohortCreator.dialect) |> q -> DBInterface.execute(sqlite_conn, q) |> DataFrame |> Array

	@test genders == GetPatientGender(test_ids, sqlite_conn)
end

@testset "GetPatientRace Tests" begin
	races = From(OMOPCDMCohortCreator.person) |> Select(Get.person_id, Get.race_concept_id) |> Limit(20) |> q -> render(q, dialect = OMOPCDMCohortCreator.dialect) |> q -> DBInterface.execute(sqlite_conn, q) |> DataFrame
	
	test_ids = From(OMOPCDMCohortCreator.person) |> Select(Get.person_id) |> Limit(20) |> q -> render(q, dialect = OMOPCDMCohortCreator.dialect) |> q -> DBInterface.execute(sqlite_conn, q) |> DataFrame |> Array

	@test races == GetPatientRace(test_ids, sqlite_conn)
end

@testset "GetPatientAgeGroup Tests" begin
	test_ids = From(OMOPCDMCohortCreator.person) |> Select(Get.person_id) |> Limit(10) |> q -> render(q, dialect = OMOPCDMCohortCreator.dialect) |> q -> DBInterface.execute(sqlite_conn, q) |> DataFrame |> Array
	
	default_age_grouping = [[0, 9], [10, 19], [20, 29], [30, 39], [40, 49], [50, 59], [60, 69], [70, 79], [80, 89]]
	test_age_grouping_1 = [[40, 59]]
	test_age_grouping_2 = [[45, 49], [50, 54], [55, 59], [60, 64], [65, 69], [70, 74], [75, 79]]

	default_minuend = :now
	minuend_1 = :db
	minuend_2 = 2022
	
	default_age_grouping_values = ["0 - 9", "10 - 19", "20 - 29", "30 - 39", "40 - 49", "50 - 59", "60 - 69", "70 - 79", "80 - 89"]

	default_test = DataFrame(:person_id => [6.0, 123.0, 129.0, 16.0, 65.0, 74.0, 42.0, 187.0, 18.0, 111.0], :year_of_birth => [1963.0, 1950.0, 1974.0, 1971.0, 1967.0, 1972.0, 1909.0, 1945.0, 1965.0, 1975.0])

	default_test.age = year(now(tz"UTC")) .- default_test.year_of_birth

	age_groups = []
	for age in default_test.age 
		for (idx, grouping) in enumerate([default_age_grouping..., missing])
			if !ismissing(grouping) && (grouping[1] <= age <= grouping[2])
				push!(age_groups, default_age_grouping_values[idx])
				break
			elseif ismissing(grouping)
				push!(age_groups, missing)
			end
		end	
	end

	default_test.age_group = age_groups
	default_test = default_test[!, [:person_id, :age_group]]
	default_test.age_group = convert(Vector{Union{Missing, String}}, default_test.age_group)
	
	minuend_1_test = DataFrame(:person_id => [6.0, 123.0, 129.0, 16.0, 65.0, 74.0, 42.0, 187.0, 18.0, 111.0], :age_group => ["40 - 59", missing, "40 - 59", "40 - 59", "40 - 59", "40 - 59", missing, missing, "40 - 59", "40 - 59"])

	minuend_2_test = DataFrame(:person_id => [6.0, 123.0, 129.0, 16.0, 65.0, 74.0, 42.0, 187.0, 18.0, 111.0], :age_group => ["55 - 59", "70 - 74", "45 - 49", "50 - 54", "55 - 59", "50 - 54", missing, "75 - 79", "55 - 59", "45 - 49"])

	@test isequal(default_test, GetPatientAgeGroup(test_ids, sqlite_conn; minuend = default_minuend, age_groupings = default_age_grouping))
	@test isequal(minuend_1_test, GetPatientAgeGroup(test_ids, sqlite_conn; minuend = minuend_1, age_groupings = test_age_grouping_1))
	@test isequal(minuend_2_test, GetPatientAgeGroup(test_ids, sqlite_conn; minuend = minuend_2, age_groupings = test_age_grouping_2))
end

# @testset "GetPatientVisits Tests" begin
# end

# @testset "GetMostRecentConditions Tests" begin
# 	test_ids = 
# 
# 
# 	@test # Multiple ids
# end

# @testset "GetMostRecentVisit Tests" begin
# 	test_ids = 
# 
# 
# 	@test # Multiple ids
# end

# @testset "GetVisitCondition Tests" begin
# 	test_ids = 
# 
# 
# 	@test # Multiple ids
# 	
# end
