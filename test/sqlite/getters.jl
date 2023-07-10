@testset "GetDatabasePersonIDs Tests" begin
    # Expected IDs from Eunomia person ids
    ids = From(OMOPCDMCohortCreator.person) |> Select(Get.person_id) |> q -> render(q, dialect=OMOPCDMCohortCreator.dialect) |> q -> DBInterface.execute(sqlite_conn, q) |> DataFrame |> x -> convert(Vector{Int}, x.person_id)

    @test ids == GetDatabasePersonIDs(sqlite_conn)
end

#= TODO: Determine how to add states to Eunomia database for testing 
Currently, states are not present in the Eunomia database - it may be as simple a fix as to alter the location table and insert random state initials in the appropriate locations for each patient.
labels: tests, moderate
=#
# @testset "GetPatientState Tests" begin
# 
# end



@testset "GetDatabaseYearRange Tests" begin
    # Test to see if correct years are reported for Eunomia
    first_year = 1922
    last_year = 2019

    check_first_year, check_last_year = GetDatabaseYearRange(sqlite_conn)
    years = (first_year=check_first_year |> unix2datetime |> year, last_year=check_last_year |> unix2datetime |> year)

    @test (first_year=first_year, last_year=last_year) == years
end

@testset "GetPatientGender Tests" begin
    genders = From(OMOPCDMCohortCreator.person) |> Select(Get.person_id, Get.gender_concept_id) |> Limit(20) |> q -> render(q, dialect=OMOPCDMCohortCreator.dialect) |> q -> DBInterface.execute(sqlite_conn, q) |> DataFrame

    test_ids = From(OMOPCDMCohortCreator.person) |> Select(Get.person_id) |> Limit(20) |> q -> render(q, dialect=OMOPCDMCohortCreator.dialect) |> q -> DBInterface.execute(sqlite_conn, q) |> DataFrame |> Array

    @test genders == GetPatientGender(test_ids, sqlite_conn)
end

@testset "GetPatientRace Tests" begin
    races = From(OMOPCDMCohortCreator.person) |> Select(Get.person_id, Get.race_concept_id) |> Limit(20) |> q -> render(q, dialect=OMOPCDMCohortCreator.dialect) |> q -> DBInterface.execute(sqlite_conn, q) |> DataFrame

    test_ids = From(OMOPCDMCohortCreator.person) |> Select(Get.person_id) |> Limit(20) |> q -> render(q, dialect=OMOPCDMCohortCreator.dialect) |> q -> DBInterface.execute(sqlite_conn, q) |> DataFrame |> Array

    @test races == GetPatientRace(test_ids, sqlite_conn)
end

@testset "GetPatientAgeGroup Tests" begin
    test_ids = From(OMOPCDMCohortCreator.person) |> Select(Get.person_id) |> Limit(10) |> q -> render(q, dialect=OMOPCDMCohortCreator.dialect) |> q -> DBInterface.execute(sqlite_conn, q) |> DataFrame |> Array

    default_age_grouping = [[0, 9], [10, 19], [20, 29], [30, 39], [40, 49], [50, 59], [60, 69], [70, 79], [80, 89]]
    test_age_grouping_1 = [[40, 59]]
    test_age_grouping_2 = [[45, 49], [50, 54], [55, 59], [60, 64], [65, 69], [70, 74], [75, 79]]

    default_minuend = :now
    minuend_now = 2022

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
    default_test.age_group = convert(Vector{Union{Missing,String}}, default_test.age_group)

    minuend_now_test = DataFrame(:person_id => [6.0, 123.0, 129.0, 16.0, 65.0, 74.0, 42.0, 187.0, 18.0, 111.0], :age_group => ["55 - 59", "70 - 74", "45 - 49", "50 - 54", "55 - 59", "50 - 54", missing, "75 - 79", "55 - 59", "45 - 49"])

    @test isequal(default_test, GetPatientAgeGroup(test_ids, sqlite_conn; minuend=default_minuend, age_groupings=default_age_grouping))
    @test isequal(minuend_now_test, GetPatientAgeGroup(test_ids, sqlite_conn; minuend=minuend_now, age_groupings=test_age_grouping_2))
end

#Tests for GetPatientVisits
@testset "GetPatientVisits Tests" begin
    #test for person with multiple visits 
    test_ids = From(OMOPCDMCohortCreator.visit_occurrence) |> Select(Get.person_id) |> Limit(20) |> q -> render(q, dialect=OMOPCDMCohortCreator.dialect) |> q -> DBInterface.execute(sqlite_conn, q) |> DataFrame |> Array
    test_visits = From(OMOPCDMCohortCreator.visit_occurrence) |> Select(Get.person_id, Get.visit_occurrence_id) |> Where(Fun.in(Get.person_id, test_ids...)) |> q -> render(q, dialect=OMOPCDMCohortCreator.dialect) |> q -> DBInterface.execute(sqlite_conn, q) |> DataFrame
    @test test_visits == GetPatientVisits(test_ids, sqlite_conn)

    #test for person with single visit
    test_id = From(OMOPCDMCohortCreator.visit_occurrence) |> Select(Get.person_id) |> Limit(1) |> q -> render(q, dialect=OMOPCDMCohortCreator.dialect) |> q -> DBInterface.execute(sqlite_conn, q) |> DataFrame |> Array

    test_visit = From(OMOPCDMCohortCreator.visit_occurrence) |> Select(Get.person_id, Get.visit_occurrence_id) |> Where(Fun.in(Get.person_id, test_id...)) |> q -> render(q, dialect=OMOPCDMCohortCreator.dialect) |> q -> DBInterface.execute(sqlite_conn, q) |> DataFrame

    @test test_visit == GetPatientVisits(test_id, sqlite_conn)
end

@testset "GetMostRecentConditions Tests" begin
        # Test to get most recent conditions for multiple patients
        test_ids = [1, 110, 6]

        test_df = DataFrame(person_id = [1.0, 110.0, 6.0], condition_concept_id = [40481087.0, 260139.0, 4218389.0])

        @test test_df == GetMostRecentConditions(test_ids, sqlite_conn)

        # Test to get most recent conditions for single patient with two conditions
        test_ids = 245

        test_df = DataFrame(person_id = [245.0, 245.0], condition_concept_id = [4230399.0, 40480160.0])

        @test test_df == GetMostRecentConditions(test_ids, sqlite_conn)
end

#tests for GetMostRecentVisit
@testset "GetMostRecentVisit Tests" begin
    test_ids = From(OMOPCDMCohortCreator.visit_occurrence) |> Select(Get.person_id) |> Limit(20) |> q -> render(q, dialect=OMOPCDMCohortCreator.dialect) |> q -> DBInterface.execute(sqlite_conn, q) |> DataFrame |> Array
    data_table = From(OMOPCDMCohortCreator.visit_occurrence) |> Group(Get.person_id) |> Select(:id => Get.person_id, :count => Agg.count()) |> q -> render(q, dialect=OMOPCDMCohortCreator.dialect) |> q -> DBInterface.execute(sqlite_conn, q) |> DataFrame
    test_visits = From(OMOPCDMCohortCreator.visit_occurrence) |> Select(Get.person_id, Get.visit_occurrence_id) |> Where(Fun.in(Get.person_id, test_ids...)) |> q -> render(q, dialect=OMOPCDMCohortCreator.dialect) |> q -> DBInterface.execute(sqlite_conn, q) |> DataFrame

    #id with multiple visits = 222
    sql = From(OMOPCDMCohortCreator.visit_occurrence) |> Select(Get.person_id, Get.visit_occurrence_id, Get.visit_end_date) |> Where(Fun.in(Get.person_id, [222]...)) |> render
    result = DBInterface.execute(sqlite_conn, sql) |> DataFrame
    result.Date = Dates.unix2datetime.(result.visit_end_date)
    max_index = argmax(result.Date)
    most_recent_visit = result.visit_occurrence_id[max_index]
    evaluated_visit = (GetMostRecentVisit(222, sqlite_conn)).visit_occurrence_id
    @test most_recent_visit == evaluated_visit[1]

    #id with 1 visits = 986
    recent_visit = test_visits[in([986]).(test_visits.person_id), :].visit_occurrence_id
    evaluated_visit = (GetMostRecentVisit(986, sqlite_conn))
    evaluated_visit.visit_occurrence_id
    @test recent_visit == evaluated_visit.visit_occurrence_id
end

@testset "GetVisitConcept Tests" begin
	test_ids = From(OMOPCDMCohortCreator.visit_occurrence) |> Select(Get.visit_occurrence_id) |> Limit(20) |> q -> render(q, dialect=OMOPCDMCohortCreator.dialect) |> q -> DBInterface.execute(sqlite_conn, q) |> DataFrame |> Array
    test_concept_ids = From(OMOPCDMCohortCreator.visit_occurrence) |> Select(Get.visit_occurrence_id, Get.visit_concept_id) |> Where(Fun.in(Get.visit_occurrence_id, test_ids...)) |> q -> render(q, dialect=OMOPCDMCohortCreator.dialect) |> q -> DBInterface.execute(sqlite_conn, q) |> DataFrame

    @test test_concept_ids == GetVisitConcept(test_ids, sqlite_conn)
end

@testset "GetVisitCondition Tests" begin

    #test for 3 visit ids with only 1 condition each
    visit_codes_single = [17479.0, 18192.0, 18859.0]
    test_condition_ids = [4.112343e6, 192671.0, 28060.0]
    test_df_single = DataFrame(visit_occurrence_id=[17479.0, 18192.0, 18859.0], condition_concept_id=[4.112343e6, 192671.0, 28060.0])

    evaluated_result_single = GetVisitCondition(visit_codes_single, sqlite_conn)
    @test test_df_single == evaluated_result_single

    #test for person with multiple visits
    visit_codes_multiple = [3.0]
    test_condition_ids_multiple = [372328.0, 81893.0]
    test_df_multiple = DataFrame(visit_occurrence_id=[3.0, 3.0], condition_concept_id=[372328.0, 81893.0])

    evaluated_result_multiple = GetVisitCondition(visit_codes_multiple, sqlite_conn)
    @test test_df_multiple == evaluated_result_multiple

end

@testset "GetPatientEthnicity Tests" begin
	ethnicities = From(OMOPCDMCohortCreator.person) |> Select(Get.person_id, Get.ethnicity_concept_id) |> Limit(20) |> q -> render(q, dialect = OMOPCDMCohortCreator.dialect) |> q -> DBInterface.execute(sqlite_conn, q) |> DataFrame

	test_ids = From(OMOPCDMCohortCreator.person) |> Select(Get.person_id) |> Limit(20) |> q -> render(q, dialect = OMOPCDMCohortCreator.dialect) |> q -> DBInterface.execute(sqlite_conn, q) |> DataFrame |> Array

	@test ethnicities == GetPatientEthnicity(test_ids, sqlite_conn)
end

@testset "GetVisitDate Tests" begin

    #test with interval = start
    test_visit_ids= [65475.0, 14930.0, 25743.0, 14888.0]
    test_start_dates = [840585600.0, 575510400.0, 1336953600.0, 1063497600.0]
    test_df_start = DataFrame(visit_occurrence_id=test_visit_ids, visit_start_date=test_start_dates)

    @test test_df_start == GetVisitDate(test_visit_ids, sqlite_conn, interval=Symbol("start"))

    #test with interval = end
    test_end_dates = [840672000.0, 575596800.0, 1337040000.0, 1063584000.0]
    test_df_end = DataFrame(visit_occurrence_id=test_visit_ids, visit_end_date=test_end_dates)

    @test test_df_end == GetVisitDate(test_visit_ids, sqlite_conn, interval=Symbol("end"))
    
end

@testset "GetDrugExposures Tests" begin
	Drug_exposure = From(OMOPCDMCohortCreator.drug_exposure) |> Select(Get.person_id, Get.drug_exposure_id) |> Limit(20) |> q -> render(q, dialect = OMOPCDMCohortCreator.dialect) |> q -> DBInterface.execute(sqlite_conn, q) |> DataFrame

	test_ids = From(OMOPCDMCohortCreator.person) |> Select(Get.person_id) |> Limit(20) |> q -> render(q, dialect = OMOPCDMCohortCreator.dialect) |> q -> DBInterface.execute(sqlite_conn, q) |> DataFrame |> Array

	@test Drug_exposure == GetDrugExposures(test_ids, sqlite_conn)
end

@testset "GetDrugConceptIDs Tests" begin
	drug_concept_ids = From(OMOPCDMCohortCreator.drug_exposure) |> Select(Get.drug_exposure_id, Get.drug_concept_id) |> Limit(20) |> q -> render(q, dialect = OMOPCDMCohortCreator.dialect) |> q -> DBInterface.execute(sqlite_conn, q) |> DataFrame

	drug_exposure_ids = From(OMOPCDMCohortCreator.drug_exposure) |> Select(Get.drug_exposure_id) |> Limit(20) |> q -> render(q, dialect = OMOPCDMCohortCreator.dialect) |> q -> DBInterface.execute(sqlite_conn, q) |> DataFrame |> Array

	@test drug_concept_ids == GetDrugExposures(drug_exposure_ids, sqlite_conn)
end

@testset "GetDrugConceptIDs Tests" begin
	drug_amounts = From(OMOPCDMCohortCreator.drug_exposure) |> Select(Get.drug_concept_id, Get.amount_value) |> Limit(20) |> q -> render(q, dialect = OMOPCDMCohortCreator.dialect) |> q -> DBInterface.execute(sqlite_conn, q) |> DataFrame

	drug_concept_ids = From(OMOPCDMCohortCreator.drug_exposure) |> Select(Get.drug_concept_id) |> Limit(20) |> q -> render(q, dialect = OMOPCDMCohortCreator.dialect) |> q -> DBInterface.execute(sqlite_conn, q) |> DataFrame |> Array

	@test drug_amounts == GetDrugExposures(drug_concept_ids, sqlite_conn)
end