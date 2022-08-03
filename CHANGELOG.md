# OMOPCDMCohortCreator.jl Change Log

All notable changes to this project will be documented in this file.
 
The format is based on [Keep a Changelog](http://keepachangelog.com/)
and this project adheres to [Semantic Versioning](http://semver.org/).
 
## [0.0.2] - August 3rd, 2022
 
Made some bugfixes, docstring updates, and feature improvements.
 
### Added

- Exports for filter functions were added
 
### Changed

- Type signatures were removed
	- Realized it was too premature to have them
 
### Fixed

- GetPatientVisits did not actually return visit ids
	- Updated to return ids from `visit_occurrence_id` versus `visit_concept_id`
 
