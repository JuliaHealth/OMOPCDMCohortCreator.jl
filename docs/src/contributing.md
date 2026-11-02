# Contributing to OMOPCDMCohortCreator 😁

OMOPCDMCohortCreator is currently under heavy development as we push to a primary release version. 
We follow a workflow pattern that is directly inspired by the [development workflow guide](http://docs.juliaplots.org/latest/contributing/#Development-Workflow-1) found in [`Plots.jl`](https://github.com/JuliaPlots/Plots.jl).
The general workflow we expect contributors to adhere to is as follows:

## 1. Create an Issue about the Problem 📝

If you want to add functionality or to work on a bug you found, open an issue first.
That'll save you from doing work that we may not support for OMOPCDMCohortCreator.

## 2. Fork the repo to your account 🍴

## 3. Create a branch based on what you are developing 🌳

Before making a branch, make sure to check that you are even with master via the following commands within your fork:

```sh
git fetch origin
git checkout master
git merge --ff-only origin/master
```

> The `--ff-only` flag will "fast forward" to newer commits. It will not create new merge commits.

Then, go ahead and create a branch that you could edit with the changes you want to see.
This is done by going into the root and typing: `git branch -b [name of your branch]`

## 4. Test, code, and commit ✏️

Once you have a fork, it is useful to make sure the fork was successful.
To verify that everything is operational, let's test it.
The following procedure is as follows:

1. Go into the root of your fork:

`cd OMOPCDMCohortCreator`

2. Open your Julia REPL and type the following within the repo:

```julia-repl
julia> ]
(@v###) pkg> activate .
(@v###) pkg> test
```

This might take some time, but if the installation on your computer is successful, it should say all tests passed.

After making the changes you wanted to make, run the tests again to make sure you did not introduce any breaking changes.
If everything passed, we can continue on to the next step.
If not, it is the responsibility of the contributor to resolve any conflicts or failing tests.
Don't worry!
We're happy to help you resolve errors. 😄
If you are stuck, go ahead and continue with this tutorial.

The way we do this is in three steps:

1. Add the files you have added or changed via `git add` 

2. After adding the files, we need to say what you did to the files (i.e. commit the files). This can be accomplished thusly: `git commit -m "your message"` 

3. Finally, let's push these changes to GitHub using `git push --set-upstream origin [name of the branch you made]`

## 5. Submitting your changes to the main project ✅

Almost done! Go to your fork and there should be a section that asks you to make a pull request (PR) from your branch. This allows the maintainers of OMOPCDMCohortCreator to see if they can add your changes to the main project. If not, you can click the "New pull request" button.

Make sure the "base" branch is OMOPCDMCohortCreator `dev` and the "compare" branch is the branch on your fork. 
To your PR, add an informative title and description, and link your PR to relevant issues or discussions. 
Finally, click "Create pull request". 

You may get some questions about it, and possibly suggestions of how to make it ready to go into the main project. 
If you had test errors or problems, we are happy to help you. 
Then, if all goes according to plan, it gets merged... **Thanks for the contribution!!** 🎉 🎉 🎉

## Creating a New Function 🛠️

Most functions in OMOPCDMCohortCreator are small queries over one OMOP CDM table.
This section walks through adding one, using a hypothetical `GetPatientYearOfBirth` that returns the `year_of_birth` of each patient.

### Setting up a test database

[HealthSampleData.jl](https://github.com/JuliaHealth/HealthSampleData.jl) provides Eunomia, a small synthetic OMOP CDM database in SQLite that the test suite also uses.
In a Julia session with `HealthSampleData`, `OMOPCDMCohortCreator`, `SQLite`, `DataFrames`, `DBInterface` and `FunSQL` available, connect to it and generate the tables:

```julia
using HealthSampleData
using OMOPCDMCohortCreator
using SQLite
using DataFrames
using DBInterface
using FunSQL: From, Get, Select, Where, Fun, render

conn = SQLite.DB(Eunomia())
GenerateDatabaseDetails(:sqlite, "main")
tables = GenerateTables(conn; exported = true)
```

`GenerateTables` with `exported = true` returns a `Dict` of FunSQL table representations, so you can build and try out queries yourself:

```julia
q = From(tables[:person]) |> Select(Get.person_id, Get.year_of_birth)
DBInterface.execute(conn, String(render(q, dialect = :sqlite))) |> DataFrame
```

If you are new to FunSQL, the [FunSQL.jl tutorials](https://mechanicalrabbit.github.io/FunSQL.jl/stable/) and the [beginner tutorial](https://juliahealth.org/OMOPCDMCohortCreator.jl/stable/tutorials/beginner_tutorial/) for this package are good places to start.

### Writing the function

Getters live in `src/getters.jl` and usually have three methods.
Inside the package, the tables (such as `person`) and `dialect` are available as globals once `GenerateDatabaseDetails` and `GenerateTables` have been called.

1. A method that only takes the IDs and returns the SQL as a `String`:

```julia
function GetPatientYearOfBirth(
    ids;
    tab = person
)

    sql =
        From(tab) |>
        Where(Fun.in(Get.person_id, ids...)) |>
        Select(Get.person_id, Get.year_of_birth) |>
        q -> render(q, dialect=dialect)

    return String(sql)

end
```

2. A method that also takes a connection, runs the query and returns a `DataFrame`:

```julia
function GetPatientYearOfBirth(
    ids,
    conn;
    tab = person
)

    df = DBInterface.execute(conn, GetPatientYearOfBirth(ids; tab=tab)) |> DataFrame

    return df

end
```

3. A method that takes a `DataFrame` with a `:person_id` column and joins the result onto it, so calls can be chained:

```julia
function GetPatientYearOfBirth(
    df::DataFrame,
    conn;
    tab = person
)

    df_ids = df[:, "person_id"]

    return outerjoin(GetPatientYearOfBirth(df_ids, conn; tab=tab), df, on = :person_id)

end
```

With all three, `GetPatientYearOfBirth(GetPatientGender([1, 2, 3], conn), conn)` returns `person_id`, `year_of_birth` and `gender_concept_id` together.

Add the function name to the `export` line at the end of `src/getters.jl`.

### Documenting the function

Give each method a docstring with `# Arguments:`, `# Keyword Arguments:` and `# Returns` sections, following the existing getters.
Then add the function name to the matching `@docs` block in `docs/src/api.md` so it appears in the API reference.

### Testing the function

Tests for getters are in `test/sqlite/getters.jl` and run against Eunomia.
A common pattern is to compare the result of the new function with the same query written directly in FunSQL:

```julia
@testset "GetPatientYearOfBirth Tests" begin

    test_ids = [1, 2, 3]

    expected = From(OMOPCDMCohortCreator.person) |> Where(Fun.in(Get.person_id, test_ids...)) |> Select(Get.person_id, Get.year_of_birth) |> q -> render(q, dialect = OMOPCDMCohortCreator.dialect) |> q -> DBInterface.execute(sqlite_conn, q) |> DataFrame

    @test sort(expected) == sort(GetPatientYearOfBirth(test_ids, sqlite_conn))

end
```

Run the full test suite as described in step 4 before opening your PR, and mention in the PR which issue the function addresses.

## Note on Adding Dependencies 📚

As a rule, we try to avoid having too many dependencies.
Therefore, we request that if you have a PR that adds a new dependency, please have opened an issue previously.

### Adding Core Dependencies 📒

If you are working on introducing a new core dependency, make sure to add that dependency to the main `Project.toml` for `OMOPCDMCohortCreator`.
To do this, follow these steps:

1. Enter the root of the `OMOPCDMCohortCreator` directory 

```sh
cd /path/to/OMOPCDMCohortCreator.jl
```

2. Activate the `OMOPCDMCohortCreator` environment and add the dependency:

```julia-repl
julia> ]
(@v###) pkg> activate .
(OMOPCDMCohortCreator) pkg> add [NAME OF DEPENDENCY]
```

### Adding Test Dependencies 📋

If you are  introducing a new test dependency, make sure to add that dependency to the `Project.toml` located in the `OMOPCDMCohortCreator` test directory.
To do this, follow these steps:

1. Enter the test directory inside of the `OMOPCDMCohortCreator` directory 

```sh
cd /path/to/OMOPCDMCohortCreator.jl/test/
```

2. Activate the `OMOPCDMCohortCreator` test environment and add the dependency:

```julia
julia> ]
(@v###) pkg> activate .
(test) pkg> add [NAME OF DEPENDENCY]
```
