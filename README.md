# OMOPCDMCohortCreator

[![Dev](https://img.shields.io/badge/docs-dev-blue.svg)](https://juliahealth.org/OMOPCDMCohortCreator.jl/dev/)
[![Test Coverage](https://codecov.io/gh/JuliaHealth/OMOPCDMCohortCreator.jl/branch/main/graph/badge.svg)](https://codecov.io/gh/JuliaHealth/OMOPCDMCohortCreator.jl)
[![Zulip](https://img.shields.io/badge/zulip-join_chat-brightgreen.svg)](https://julialang.zulipchat.com/#narrow/stream/237221-biology-health-and-medicine)
[![Build Status](https://github.com/JuliaHealth/OMOPCDMCohortCreator.jl/workflows/CI/badge.svg)](https://github.com/JuliaHealth/OMOPCDMCohortCreator.jl/actions)
[![Code Style: Blue](https://img.shields.io/badge/code%20style-blue-4495d1.svg)](https://github.com/invenia/BlueStyle)
[![DOI](https://zenodo.org/badge/508759910.svg)](https://zenodo.org/badge/latestdoi/508759910)

> Create cohorts from databases utilizing the OMOP CDM.

This is a **work in progress package** that allows one to interface with an OMOP CDM database for observational health research and analytics.
This package works on version 5.4 of the OMOP CDM and provides a number of filters and getter functions.

For questions, feel free to [start a discussion](https://github.com/JuliaHealth/OMOPCDMCohortCreator.jl/discussions), create an [issue](https://github.com/JuliaHealth/OMOPCDMCohortCreator.jl/issues), or post on [Zulip](https://github.com/JuliaHealth/OMOPCDMCohortCreator.jl/discussions).

This package uses a functional approach to query databases in the [OMOP Common Data Model format](https://www.ohdsi.org/data-standardization/the-common-data-model/) whereby you could build rapidly lines of inquiry into the database.
Furthermore, this package is a companion to those tools found in the [HADES](https://ohdsi.github.io/Hades/) ecosystem.
Documentation has been set up [so please check them out](https://juliahealth.org/OMOPCDMCohortCreator.jl)!
To get started, visit the [Tutorials](https://juliahealth.org/OMOPCDMCohortCreator.jl/tutorials) section as well as visit the [API](https://juliahealth.org/OMOPCDMCohortCreator.jl/api) section to see all the functions available.
If you want to contribute, please check out our [Contributing](https://juliahealth.org/OMOPCDMCohortCreator.jl/contributing) guide!

# Main Features 🔧

The biggest features of this package are:

- Incremental building blocks for creating an analysis pipeline in the form of (more information in [API](https://juliahealth.org/OMOPCDMCohortCreator.jl/api)):
  - "Getter" functions to "get" information from a database 
  - "Filter" functions to "filter" information from a database 
  - "Generator" functions to "generate" database information and connections
  - "Executor" functions to "execute" on retrieved information 
- Automatic targeting and support for the SQL flavors (via [FunSQL.jl](https://mechanicalrabbit.github.io/FunSQL.jl/)):
  - `postgresql`
  - `sqlite`
  - `redshift`
- Prepare SQL queries if unable to connect to database via OMOPCDMCohortCreator that could then be run on a given SQL database directly
- Does not mutate database or require temp tables
- Interoperable with the R language via [JuliaConnectoR](https://github.com/stefan-m-lenz/JuliaConnectoR) to work directly within R syntax (see [Tutorials](https://juliahealth.org/OMOPCDMCohortCreator.jl/tutorials))
- Readily parallelizable via [Distributed.jl](https://docs.julialang.org/en/v1/manual/distributed-computing/)
- Complementary to [OHDSI HADES](https://ohdsi.github.io/Hades/) ecosystem tools
- Extensive [test suite](https://github.com/JuliaHealth/OMOPCDMCohortCreator.jl/tree/main/test) to ensure correctness and compliance with privacy preserving methods (HITECH, etc.)

# Maintainership 👷

This package is currently maintained by Jacob S. Zelko (AKA @TheCedarPrince) as of October 2022.
If this repository should fall out of maintainership and there is an urgent need, please contact members of the [JuliaHealth organization](https://github.com/JuliaHealth) for assistance.
Thank you!

# Citation Information 📝

```tex
@software{Zelko_OMOPCDMCohortCreator_0_2_0_2022,
author = {Zelko, Jacob and Chinta, Varshini},
doi = {10.5281/zenodo.7052105},
month = {10},
title = {{OMOPCDMCohortCreator 0.2.0}},
version = {0.2.0},
year = {2022}
}
```
