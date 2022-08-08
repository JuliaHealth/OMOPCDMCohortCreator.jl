# Tutorials

```@index
```

## Beginner Tutorial

### Environment Set-Up

For this tutorial, you will need to activate an environment; to get into package mode within your Julia REPL, write `]`:

```julia-repl
pkg> activate TUTORIAL
```

#### Packages 

You will need the following packages for this tutorial which you can install in package mode:

```julia-repl
TUTORIAL> add OMOPCDMCohortCreator
TUTORIAL> add OMOPCDMDatabaseConnector
TUTORIAL> add SQLite
TUTORIAL> add DataFrames
TURORIAL> add SampleHealthData
```

> **NOTE:** For `SampleHealthData`, the package is in the process of being registered so for now, you may have to instead do if the `add` command fails.

> 
> ```julia-repl
> TUTORIAL> add https://github.com/JuliaHealth/HealthSampleData.jl
> ```


<!--TODO: Add descriptions on what these packages are and what they are for -->

#### Data 

For this tutorial, we will work with data from [Eunomia](https://github.com/OHDSI/Eunomia) that is stored in a SQLite format.
To install the data on your machine, execute the following code block and follow the prompts - you will need a stable internet connection for the download to complete: 

```julia
using SampleHealthData

eunomia = Eunomia()
```
### Connecting to the Eunomia Database

After you have finished your set up in the Julia, we need to establish a connection to the Eunomia SQLite database that we will use for the rest of the tutorial: 

```julia
using SQLite

conn = SQLite.DB(eunomia)
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

As a check to make sure everything was correctly installed and works properly, the following block should work and return a list of all person ids in this data:

```julia
GetDatabasePersonIDs(conn)
```

### Characterizing Patients Who Have Had Strep Throat

#### Background 

As all the tools are working properly, let's do what is called a characterization study - a study that *characterizes* a group of patients with a certain condition (or conditions) across various attributes like race, age, and combinations thereof.
We are going to do miniature version of such a study looking at patients with strep throat.
For this, we will use the `condition_concept_id`: $28060$ - this will be needed for you to get correct results.

> **NOTE:** As we go through this example, do not immediately jump to the code block required but try to use the [API] to come up with solutions as there may be more than one answer.

<!--TODO: Add link to API page-->

#### Task: Find All Patients with Strep Throat

Using the API, find all patients with strep throat.

Suggested solution:

```julia
strep_patients = ConditionFilterPersonIDs(28060, conn)
```

#### Task: Find the Race of Patients with Strep Throat

For the patients who have strep throat diagnoses, find their race.

Suggested solution:

```julia
strep_patients_race = GetPatientRace(strep_patients, conn)
```

#### Task: Find the Gender of Patients with Strep Throat

For the patients who have strep throat diagnoses, find their gender.

Suggested solution:

```julia
strep_patients_race = GetPatientGender(strep_patients, conn)
```

#### Task: Create Age Groupings of Patients with Strep Throat

For this task, for every single person who has a strep throat diagnosis, assign them an age group.
The age groupings must follow $5$ year intervals when assigned to a person up to $100$ years of age (e.g. $[0, 4], [5, 9], ... [95, 100]$).

Suggested solution:

```julia
age_groups = [
	[0, 4],
	[5, 9],
	[10, 14],
	[15, 19],
	[20, 24],
	[25, 29],
	[30, 34],
	[35, 39],
	[40, 44],
	[45, 49],
	[50, 54],
	[55, 59],
	[60, 64],
	[65, 69],
	[70, 74],
	[75, 79],
	[80, 84],
	[85, 89],
	[90, 94],
	[95, 99]
]
strep_patients_age_group = GetPatientAgeGroup(strep_patients, conn; age_groupings = age_groups)
```

#### Task: Characterize Each Person by Gender, Race, and Age Group

With the previous tasks, we now know patients' gender, race, and age group.
Using this information, combine these features to create a final table where each patient's `person_id`, gender, race, and age group is found in a given row.
Hint: The DataFrames.jl documentation section on joins will be of strong use here.

Suggested solution:

```julia
strep_patients_age_group = GetPatientAgeGroup(strep_patients)
```
<!--TODO: Add solution here-->
<!--TODO: Add link to DataFrames documentation-->

#### Task: Create Patient Groupings

Often with characterization style studies, it is extremely important to aggregate patient populations.
Why?
To protect the anonymity of patients with perhaps severely sensitive conditions (e.g. mental illnesses, sexually transmitted diseases, etc.) from possible repercussions from accidental disclosure of this patient information.

For this task, add to the table you created in the previous task a new column called `counts` and remove the `person_id` column.
The `counts` column should represent the total number of patients belonging to a group's gender, race, and age group.
Here is an example on how to calculate `counts`: if there are $5$ rows in your table that have patients who are between the ages of $20 - 24$, are African American, and are female, the value for that age, race, and gender group is $5$.
The $5$ rows would then collapse into $1$ row as unique patient identifiers (the `person_id` column) would be removed. Hint: removing the `person_id` column first may make things easier.

Suggested solution:

```julia

```
<!--TODO: Add solution here-->
