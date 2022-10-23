@testset "VisitFilterPersonIDs Tests" begin
    #9201 was the only visit_concept id for Eunomia
    VisitFilterPersonIDs([9201.0], sqlite_conn)
    test_person_ids = [1, 2, 3, 5, 32, 35, 36, 42, 61, 80]

    res = sort(VisitFilterPersonIDs([9201.0], sqlite_conn))
    @test test_person_ids == res.person_id[1:10]
end

@testset "ConditionFilterPersonIDs Tests" begin
    #test for 192671.0 - singe condition code
    test_person_ids_single = [3, 32, 35, 61, 80, 99, 115, 116, 133, 135]
    res =  sort(ConditionFilterPersonIDs([192671.0], sqlite_conn))
    @test test_person_ids_single == res.person_id[1:10]

    #test for multiple condition codes - 28060 and 192671
    test_person_ids_multiple = [1, 3, 5, 9, 11, 12, 17, 18, 19, 32]
    res_multiple = sort(ConditionFilterPersonIDs([192671.0, 28060.], sqlite_conn))

    @test test_person_ids_multiple == res_multiple.person_id[1:10]
	
end

@testset "RaceFilterPersonIDs Tests" begin
    #test for single ID - check if output matches that of querying from Eunomia
    #sql command: > select distinct(person_id), race_concept_id from person where race_concept_id = 8516.0 order by person_id asc limit 10;
    race_id_single = [8516.0]
    person_list_8516 = [6.0,
    9.0,
    41.0,
    65.0,
    81.0,
    99.0,
    103.0,
    148.0,
    164.0,
    190.0]
    res_single = sort(RaceFilterPersonIDs(race_id_single, sqlite_conn)).person_id[1:10]
    @test person_list_8516 == res_single

    #test for multiple IDs
    #sql command:  select distinct(person_id) from person where race_concept_id = 8516.0 or race_concept_id = 8515.0 or race_concept_id = 8527.0 order by person_id asc limit 10;
    race_ids_multiple = [8516.0, 8527.0, 8515.0]
    person_list_multiple = [1.0,
    2.0,
    3.0,
    5.0,
    6.0,
    9.0,
    11.0,
    12.0,
    16.0,
    17.0]
    res_multiple = sort(RaceFilterPersonIDs(race_ids_multiple, sqlite_conn)).person_id[1:10]
    @test person_list_multiple == res_multiple
end

@testset "GenderFilterPersonIDs Tests" begin
   #sql to get ids: select distinct(gender_concept_id) from person limit 10;
   gender_single = [8532.0]
   #sql: select distinct(person_id) from person where gender_concept_id = 8532.0 order by person_id asc limit 10;
   person_list_genders = [2.0,
   6.0,
   7.0,
   9.0,
   12.0,
   16.0,
   17.0,
   18.0,
   19.0,
   30.0]
   res_single = sort(GenderFilterPersonIDs(gender_single, sqlite_conn)).person_id[1:10]
   @test person_list_genders == res_single

   gender_id_multiple = [8532, 8507.0]
   #select distinct(person_id) from person where gender_concept_id = 8532.0 or gender_concept_id = 8507.0 order by person_id asc limit 10;
   person_list_multiple = [1.0,
   2.0,
   3.0,
   5.0,
   6.0,
   7.0,
   9.0,
   11.0,
   12.0,
   16.0]
   res_multiple = sort(GenderFilterPersonIDs(gender_id_multiple, sqlite_conn)).person_id[1:10]
   @test person_list_multiple == res_multiple
end

#= TODO: Add tests to StateFilterPersonIDs
This depends on getting the test set for GetPatientState sorted out
labels: tests, moderate
=#
# @testset "StateFilterPersonIDs Tests" begin
#	
# end
