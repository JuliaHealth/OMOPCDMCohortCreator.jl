#= TODO: Add tests to VisitFilterPersonIDs 
**Description:** Add two tests - one for a single ID and another for multiple IDs. Reference the tests in GetPatientGender.
labels: tests, good first issue
=#
@testset "VisitFilterPersonIDs Tests" begin
    #9201 was the only visit_concept id?
    VisitFilterPersonIDs([9201.0], sqlite_conn)
    test_person_ids = [1, 2, 3, 5, 32, 35, 36, 42, 61, 80]

    res =  sort(VisitFilterPersonIDs([9201.0], sqlite_conn))
    @test test_person_ids == res[1:10]
end

#= TODO: Add tests to ConditionFilterPersonIDs
**Description:** Add two tests - one for a single ID and another for multiple IDs. Reference the tests in GetPatientGender.
labels: tests, good first issue
=#
@testset "ConditionFilterPersonIDs Tests" begin
    #test for 192671.0 - singe condition code
    test_person_ids_single = [3, 32, 35, 61, 80, 99, 115, 116, 133, 135]
    res =  sort(ConditionFilterPersonIDs([192671.0], sqlite_conn))
    @test test_person_ids_single == res[1:10]

    #test for multiple condition codes - 28060 and 192671
    test_person_ids_multiple = [1, 3, 5, 9, 11, 12, 17, 18, 19, 32]
    res_multiple = sort(ConditionFilterPersonIDs([192671.0, 28060.], sqlite_conn))[1:10]
    @test test_person_ids_multiple == res_multiple[1:10]
	
end

#= TODO: Add tests to RaceFilterPersonIDs
**Description:** Add two tests - one for a single ID and another for multiple IDs. Reference the tests in GetPatientGender.
labels: tests, good first issue
=#
@testset "RaceFilterPersonIDs Tests" begin
	
end

#= TODO: Add tests to GenderFilterPersonIDs
**Description:** Add two tests - one for a single ID and another for multiple IDs. Reference the tests in GetPatientGender.
labels: tests, good first issue
=#
@testset "GenderFilterPersonIDs Tests" begin
	
end

#= TODO: Add tests to StateFilterPersonIDs
This depends on getting the test set for GetPatientState sorted out
labels: tests, moderate
=#
@testset "StateFilterPersonIDs Tests" begin
	
end
