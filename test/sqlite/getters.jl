@testset "GetDatabasePersonIDs Tests" begin
	#TODO: Generate list of person_ids for testing against
	person_table_person_ids = 
	visit_occurrence_table_person_ids = 

	@test person_table_person_ids == GetDatabasePersonIDs(sqlite_conn)
	@test visit_occurrence_table_person_ids == GetDatabasePersonIDs(sqlite_conn; tab = visit_occurrence)
end

#TODO: Determine best way to create tests for any location based test
@testset "GetPatientState Tests" begin

end

@testset "GetPatientGender Tests" begin
	#TODO: Create a list of test_ids
	test_ids = 
	single_id = 42

	#TODO: Create matching DataFrame for test_ids that has genders

	@test # Multiple ids
	@test # Single ids
end

@testset "GetPatientRace Tests" begin
	#TODO: Create a list of test_ids
	test_ids = 
	single_id = 42

	#TODO: Create matching DataFrame for test_ids that has races

	@test # Multiple ids
	@test # Single ids
end

@testset "GetPatientAgeGroup Tests" begin
	#TODO: Create a list of test_ids
	test_ids = 
	single_id = 42

	default_age_grouping = [[0, 9], [10, 19], [20, 29], [30, 39], [40, 49], [50, 59], [60, 69], [70, 79], [80, 89]]
	test_age_grouping_1 = [[30, 39]]
	test_age_grouping_2 = [[20, 24], [25, 29], [30, 34], [35, 39], [40, 44], [45, 49]]

	default_minuend = :now
	minuend_2 = :db
	minuend_3 = 2022

	#TODO: Create matching DataFrame for test_ids that has races

	@test # Multiple ids
	@test # Single ids
end

#TODO: Create GetPatientVisits tests after refactoring
@testset "GetPatientVisits Tests" begin
end

@testset "GetMostRecentConditions Tests" begin
	#TODO: Create a list of test_ids
	test_ids = 

	#TODO: Create matching DataFrame for test_ids that has genders

	@test # Multiple ids
end

@testset "GetMostRecentVisit Tests" begin
	#TODO: Create a list of test_ids
	test_ids = 

	#TODO: Create matching DataFrame for test_ids that has genders

	@test # Multiple ids
end

@testset "GetVisitCondition Tests" begin
	#TODO: Create a list of test_ids
	test_ids = 

	#TODO: Create matching DataFrame for test_ids that has genders

	@test # Multiple ids
	
end
