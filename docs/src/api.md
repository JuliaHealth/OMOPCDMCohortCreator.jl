# API

This is a list of documentation associated with every single **exported** function from `OMOPCDMCohortCreator`.
There are a few different sections with a brief explanation of what these sections are followed by relevant functions.

```@contents
Pages = ["api.md"]
```


## Getters

This family of functions are dedicated to only getting information concerning a patient or OMOP CDM database.

```@docs
GetDatabasePersonIDs
GetPatientState
GetPatientGender
GetPatientRace
GetPatientEthnicity
GetPatientAgeGroup
GetPatientVisits
GetMostRecentConditions
GetMostRecentVisit
GetVisitCondition
GetDatabaseYearRange
```

## Filters

These functions accepts parameters to produce queries that look for specific subpopulations or information given specific patient identifier(s) (i.e. `person_id`). 

```@docs
VisitFilterPersonIDs
ConditionFilterPersonIDs
RaceFilterPersonIDs
GenderFilterPersonIDs
StateFilterPersonIDs
AgeGroupFilterPersonIDs
```

## Generators

The generator functions are to set generate initial connections to an OMOP CDM database or to finalize resulting data from queries into various outputs. 

```@docs
GenerateDatabaseDetails
GenerateGroupCounts
GenerateTables
```

## Executors

These functions perform quality assurance checks on data extracts genereated from OMOPCDMCohortCreator queries.

```@docs
ExecuteAudit
```
