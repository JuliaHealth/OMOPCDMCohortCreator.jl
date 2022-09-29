#= TODO: Add tests to VisitFilterPersonIDs 
**Description:** Add two tests - one for a single ID and another for multiple IDs. Reference the tests in GetPatientGender.
labels: tests, good first issue
=#
@testset "VisitFilterPersonIDs Tests" begin
    #9201 was the only visit_concept)id?
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
