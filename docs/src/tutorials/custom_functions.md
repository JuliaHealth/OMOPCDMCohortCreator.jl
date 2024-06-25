# Beginner Tutorial 🐣

```@contents
Pages = ["custom_functions.md"]
```

This tutorial presents a step by step guide on using OMOPCDMCohortCreator to run a mini characterization study!
You will learn the basics of OMOPCDMCohortCreator and how to use it with fake data that you could then apply to your real data sets.
Basic knowledge of Julia (such as installing packages into environments and working with the Julia REPL and Julia files) is necessary; you can learn all [that here](https://pkgdocs.julialang.org/v1/getting-started/).

## Environment Set-Up 📝

For this tutorial, you will need to activate an environment; to get into package mode within your Julia REPL, write `]`:

```julia-repl
pkg> activate TUTORIAL
```

### Packages 

You will need the following packages for this tutorial which you can install in package mode:

```julia-repl
TUTORIAL> add OMOPCDMCohortCreator
TUTORIAL> add SQLite
TUTORIAL> add DataFrames
TURORIAL> add HealthSampleData
```

To learn more about these packages, see the [Appendix](#appendix).

### Data 

For this tutorial, we will work with data from [Eunomia](https://github.com/OHDSI/Eunomia) that is stored in a SQLite format.
To install the data on your machine, execute the following code block and follow the prompts - you will need a stable internet connection for the download to complete: 

```julia
import HealthSampleData: Eunomia

eunomia = Eunomia()
```

## Connecting to the Eunomia Database 💾

After you have finished your set up in the Julia, we need to establish a connection to the Eunomia SQLite database that we will use for the rest of the tutorial: 

```julia
import SQLite: DB

conn = DB(eunomia)
```

With Eunomia, the database's schema is simply called "main".
We will use this to generate database connection details that will inform `OMOPCDMCohortCreator` about the type of queries we will write (i.e. SQLite) and the name of the database's schema.
For this step, we will use `OMOPCDMCohortCreator`:

```julia
import OMOPCDMCohortCreator as occ

occ.GenerateDatabaseDetails(
    :sqlite,
    "main"
)
```

Finally, we will generate internal representations of each table found within Eunomia for OMOPCDMCohortCreator to use:

```julia
occ.GenerateTables(conn)
```

As a check to make sure everything was correctly installed and works properly, the following block should work and return a list of all person ids in this data:

```julia
occ.GetDatabasePersonIDs(conn)
```

## Characterizing Patients Who Have Had Strep Throat 🤒

### Background 

### Details

how can i execute sql statements against generated tables i had done
stmt = DBInterface.prepare(conn, """SELECT * FROM  omop.procedure_occurrence  LIMIT 1""") # prepare a sql statement against the connection; returns a statement object
results = DBInterface.execute(stmt) # execute a prepared statement; returns an iterator of rows (property-accessible & indexable)
result.column_names
but it prints just
2-element Vector{String}:
 "no_nulls"
 "yes_nulls"


Jakub Mitura
  1 day ago
I had looked into your code and you seem to use FunSQL which seem really nice library however when i try to replicate your code I can not get reference to table
code:
using FunSQL: From,Limit,render

conn = connect(
    Connection, 
    "host=localhost port=5432 dbname=synthea user=thecedarprince password=pass*"
)    
GenerateDatabaseDetails(:postgresql, "omop")
GenerateTables(conn)

q = From(:procedure_occurrence)|>
Limit(1) |>
q -> render(q, dialect=OMOPCDMCohortCreator.dialect)
although during initialization I get
[ Info: procedure_occurrence table generated internally
I get error
 FunSQL.ReferenceError: cannot find `procedure_occurrence`
Thanks for patience !
Saved for later • Due 24 hours ago


Jakub Mitura
  1 day ago
@TheCedarPrince
 the funsql way seem very nice just do not have idea how to access tables that during initialization logging info stated it was created


TheCedarPrince
:deciduous_tree:  1 day ago
Hey 
@Jakub Mitura
 -- I'll write a tutorial and add that it into the documentation but the short answer to this is as follows.
:thumbsup_all:
1



TheCedarPrince
:deciduous_tree:  1 day ago
using OMOPCDMCohortCreator

# Create connection here
# Other code
GenerateDatabaseDetails(:postgresql, "omop")
tables = GenerateTables(conn, inplace = false, exported = true)
Then, within that tables variable, you can get FunSQL representations of the tables you want:
person = tables[:person]
:thumbsup_all:
1



TheCedarPrince
:deciduous_tree:  1 day ago
Then you can write queries such as:
sql = From(person) |> Select(Get.person_id) |> x -> render(x, :postgresql)
DBInterface.execute(conn, sql) |> DataFrame
:thumbsup_all:
1



TheCedarPrince
:deciduous_tree:  1 day ago
Hopefully that should get you unstuck 
@Jakub Mitura
 and sorry for the delay in response.
:thumbsup_all:
1



TheCedarPrince
:deciduous_tree:  1 day ago
Additionally, the FunSQL reference documentation is fantastic here: https://mechanicalrabbit.github.io/FunSQL.jl/dev/guide/

## Conclusion 🎉

This mini characterization study that we just conducted on this dataset opens up a whole new avenue for a researcher to pursue.
For example, we could now calculate prevalence rates across different patient characteristics or compare and contrast multiple conditions at once.
It should also be apparent that the API is set up in a very particular way: it is functional meaning that each function does one thing only.
This gives a lot of flexibility to a user to build together study incrementally using OMOPCDMCohortCreator.
Congratulations on finishing this tutorial and if there are any issues you encountered, [feel free to open an issue here](https://github.com/JuliaHealth/OMOPCDMCohortCreator.jl/issues/new/choose)!

## Appendix 🕵️

### Packages Used in Analysis

Package descriptions:

- [`DataFrames`](https://github.com/JuliaData/DataFrames.jl) - Julia's dataframe handler for easily manipulating data

- [`OMOPCDMCohortCreator`](https://github.com/JuliaHealth/OMOPCDMCohortCreator.jl) - Create cohorts from databases utilizing the OMOP CDM

- [`HealthSampleData`](https://github.com/JuliaHealth/HealthSampleData.jl) - Sample health data for a variety of health formats and use cases

- [`SQLite`](https://github.com/JuliaDatabases/SQLite.jl) - A Julia interface to the SQLite library
