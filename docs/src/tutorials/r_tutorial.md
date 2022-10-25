# Using OMOPCDMCohortCreator with R 🏴‍☠️

```@contents
Pages = ["r_tutorial.md"]
```

This tutorial builds on the [Beginner Tutorial](tutorials/beginner_tutorial.md) in creating a characterization study but instead of using Julia, we will use R!
This assumes the user has familiarity with R but familiarity with Julia is not required.
By the end of this tutorial, you will learn how to use OMOPCDMCohortCreator directly within R without having to ever touch Julia.

## Analysis Set-up 📝

### R and Julia Installation

You will need to have R and Julia installed onto your computer.
Here are the minimum required versions needed:

- R version must be greater than or equal to version $3.2$.
- Julia version must be greater than or equal to version $1.7$.

Furthermore, the Julia executable must be available from the system `PATH` or you can set the `JULIA_BINDIR` R environment variable to where the Julia `bin` directory is on your computer like this:

```r 
# For Windows
Sys.setenv(JULIA_BINDIR = "C:/Users/user/AppData/Local/Programs/Julia-1.7.1/bin")

# For Linux or OSX
Sys.setenv(JULIA_BINDIR = "~/path/to/Julia-1.7.1/bin")
```

### Packages 

#### R Packages 

You will need the following packages for this tutorial which you can install with `install.packages`:

- `dplyr`
- `JuliaConnectoR`
- `tibble`

To read more on these packages and what they do, see the [appendix](#appendix) for details.

#### Julia Packages 

You will need the following packages for this tutorial:

- `HealthSampleData`
- `OMOPCDMCohortCreator`
- `SQLite`

We will have to install them as follows from within R:

```r
library(JuliaConnectoR)

pkg <- juliaImport("Pkg")

pkg$activate("TUTORIAL", shared = TRUE) 
# NOTE: You could specify the path to where your project is or any other path you want; set `shared` to `FALSE` if you do

pkg$add(c("HealthSampleData", "OMOPCDMCohortCreator", "SQLite"))
```

To read more on these packages and what they do, see the [appendix](#appendix) for details.

#### Activating Analysis Environment

Now within R, anytime you want to use these installed packages, do the following:

```r
library(JuliaConnectoR)

pkg <- juliaImport("Pkg")
pkg$activate("TUTORIAL", shared = TRUE) 
```

### Data 

For this tutorial, we will work with data from [Eunomia](https://github.com/OHDSI/Eunomia) that is stored in a SQLite format.

```r
hsd <- juliaImport("HealthSampleData")

eunomia <- hsd$Eunomia()
```

> **NOTE:** An internet connection will be needed to download this data.
> After this data is downloaded, internet is no longer required for this tutorial.

## Create Database Connection to Eunomia 💾

After you have finished your set up in R, we need to establish a connection to the Eunomia SQLite database that we will use for the rest of the tutorial: 

```r
slt <- juliaImport("SQLite")

conn <- slt$DB(eunomia)
```

With Eunomia, the database's schema is simply called "main".
We will use this to generate database connection details that `OMOPCDMCohortCreator` will use internally.
For this step, we will use `OMOPCDMCohortCreator`:

```r
occ <- juliaImport("OMOPCDMCohortCreator")

occ$GenerateDatabaseDetails(juliaEval(":sqlite"), "main")
```

Finally, we will generate internal representations of each table found within Eunomia for OMOPCDMCohortCreator to use:

```r
occ$GenerateTables(conn)
```

As a check to make sure everything was correctly installed and works properly, the following block should work and return a list of all person ids in this data:

```r
occ$GetDatabasePersonIDs(conn) 
```

## Characterization Analysis 🤒

### Background for Analysis

As all the tools are working properly, let's do what is called a characterization study - a study that *characterizes* a group of patients with a certain condition (or conditions) across various attributes like race, age, and combinations thereof.
We are going to do miniature version of such a study looking at patients with _strep throat_.
For this, we will use the `condition_concept_id`, $28060$.

### Task: Find All Patients with Strep Throat

```r
strep_patients <- occ$ConditionFilterPersonIDs(28060, conn)
strep_patients <- strep_patients$person_id
```

### Task: Find the Race of Patients with Strep Throat

```r
strep_patients_race <- occ$GetPatientRace(strep_patients, conn)
```

### Task: Find the Gender of Patients with Strep Throat

```r
strep_patients_gender <- occ$GetPatientGender(strep_patients, conn)
```

### Task: Create Age Groupings of Patients with Strep Throat

For this task, for every single person who has a strep throat diagnosis, we need to assign them an age group.
For this demo, age groupings will be made along $5$ year intervals when assigned to a person up to $100$ years of age (e.g. $[0, 4], [5, 9], ... [95, 100]$).

```r
age_groups <- list(
	list(0, 4),
	list(5, 9),
	list(10, 14),
	list(15, 19),
	list(20, 24),
	list(25, 29),
	list(30, 34),
	list(35, 39),
	list(40, 44),
	list(45, 49),
	list(50, 54),
	list(55, 59),
	list(60, 64),
	list(65, 69),
	list(70, 74),
	list(75, 79),
	list(80, 84),
	list(85, 89),
	list(90, 94),
	list(95, 99))

strep_patients_age_group <- occ$GetPatientAgeGroup(strep_patients, conn, age_groupings = age_groups)
```

### Task: Characterize Each Person by Gender, Race, and Age Group

With the previous tasks, we now know patients' gender, race, and age group.
Using this information, we can combine these features to create a final table showing each patient's `person_id`, gender, race, and age group per row.
To do this combining, we will use the `dplyr` and `tibble` packages:

```r
library(dplyr)
library(tibble)

strep_patients <- tibble(data.frame(person_id=strep_patients))
strep_patients_race <- tibble(data.frame(strep_patients_race))
strep_patients_gender <- tibble(data.frame(strep_patients_gender))
strep_patients_age_group <- tibble(data.frame(strep_patients_age_group))
```

### Task: Create Patient Groupings

Often with characterization style studies, it is extremely important to aggregate patient populations.
This is to protect the anonymity of patients with perhaps severely sensitive conditions (e.g. mental illnesses, sexually transmitted diseases, etc.) from possible repercussions from accidental disclosure of this patient information.

```r
final_df <- full_join(strep_patients, strep_patients_race, by = c("person_id" = "person_id")) %>% 
full_join(strep_patients_age_group, by = c("person_id" = "person_id")) %>%
full_join(strep_patients_gender, by = c("person_id" = "person_id")) %>%
select(-person_id) %>% 
count(race_concept_id, age_group, gender_concept_id) %>%
rename(count = "n") %>%
filter(count > 10)
```

## Conclusion 🎉 

This mini characterization study that we just conducted on this dataset opens up a whole new avenue for a researcher to pursue.
For example, we could now calculate prevalence rates across different patient characteristics or compare and contrast multiple conditions at once.
It should also be apparent that the API is set up in a very particular way: it is functional meaning that each function does one thing only.
This gives a lot of flexibility to a user to build together a study incrementally using `OMOPCDMCohortCreator`.

## Appendix 🕵️

### Packages Used in Analysis

#### R Packages Used: 

- [`dplyr`](https://dplyr.tidyverse.org) - grammar of data manipulation
- [`tibble`](https://tibble.tidyverse.org) - modern reimagining of the data.frame
- [`JuliaConnectoR`](https://github.com/stefan-m-lenz/JuliaConnectoR) - a functionally oriented interface for calling Julia from R

#### Julia Packages Used: 

- [`OMOPCDMCohortCreator`](https://github.com/JuliaHealth/OMOPCDMCohortCreator.jl) - Create cohorts from databases utilizing the OMOP CDM
- [`HealthSampleData`](https://github.com/JuliaHealth/HealthSampleData.jl) - Sample health data for a variety of health formats and use cases
- [`SQLite`](https://github.com/JuliaDatabases/SQLite.jl) - A Julia interface to the SQLite library
