# Tutorials

```@index
```

## Beginner Tutorial

### Environment Set-Up

For this tutorial, you will need to activate an environment:

```julia-repl
pkg> activate TUTORIAL
```

#### Packages 

You will need the following packages for this tutorial:

```julia-repl
TUTORIAL> add OMOPCDMCohortCreator
TUTORIAL> add OMOPCDMDatabaseConnector
TUTORIAL> add DataDeps
TUTORIAL> add SQLite
TUTORIAL> add DataFrames
```

#### Data 

For this tutorial, we will work with data from [Eunomia](https://github.com/OHDSI/Eunomia) that is stored in a SQLite format.
For now, either run the following code block in your Julia REPL or add it to the script you are writing to follow this tutorial: 

```julia
using DataDeps

function datasets()

    register(DataDep(
        "Eunomia",
        "A standard CDM dataset for testing and demonstration purposes; link: https://github.com/OHDSI/Eunomia",
        "https://app.box.com/index.php?rm=box_download_shared_file&shared_name=n5a21tbu1rwpilgcm6q9oip30ti86ven&file_id=f_988456839468",
        "b9a6e4662107cfdbd80d96c3600f292eb480652b47ec06410158220f43326042"
    ))

end
```

Then go ahead and call the `datasets` function:

```julia
datasets()
```

The `datasets` function gives us access to a variety of datasets through the `datadep` syntax.
For our case, we are going to use the Eunomia SQLite file and assign the file to a variable that we will use later.
Execute the following code block and 

```julia
data = datadep"Eunomia/eunomia.sqlite"
```
**NOTE**: Warning: Checksum did not match
expected_hash = "b9a6e4662107cfdbd80d96c3600f292eb480652b47ec06410158220f43326042"
actual_hash = "cef9f00fe469041cfcdec527fc6f5657e2d0185bc002df38262faa642ee97c82" 
path = "C:\\Users\\jmoreland30\\.julia\\datadeps\\Eunomia\\eunomia.sqlite"
@ DataDeps C:\Users\jmoreland30\.julia\packages\DataDeps\EDWdQ\src\verification.jl:24                                                                                               Do you wish to Abort, Retry download or Ignore                                                                                                                                      [a/r/i]  
                                       
> **NOTE:** In a later release, the data download step should no longer be required or at least be automated in another package. 

### Connecting to the Eunomia Database

After you have finished your set up in the Julia, we need establish a connection to the Eunomia SQLite database that we will use for the rest of the tutorial: 

```julia
using SQLite

conn = SQLite.DB(datadep"Eunomia/eunomia.sqlite")
```

With Eunomia, the database's schema is simply called "main".
We will use this to generate database connection details that will inform `OMOPCDMCohortCreator` about the type of queries we will write (i.e. SQLite) and the name of the database's schema.
For this step, we will use `OMOPCDMCohortCreator` and `OMOPCDMDatabaseConnector`:

```julia
using OMOPCDMCohortCreator
using OMOPCDMDatabaseConnector

GenerateDatabaseDetails(
    :sqlite,
    "main"
)
```

Finally, we will generate internal representations of each table found within Eunomia for OMOPCDMCohortCreator to use:

```julia
GenerateTables(conn)
```

As a check to make sure everything was correctly installed and works properly, the following block should work:

```julia
GetDatabasePersonIDs(conn)
```

Strep throat: 28060
Concussions: 378001
