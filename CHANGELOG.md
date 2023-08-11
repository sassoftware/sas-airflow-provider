# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). If you introduce breaking changes, please group them together in the "Changed" section using the **BREAKING:** prefix.

## [v0.0.7] - 2023-07-31

### Added

- Allow passing airflow vars as macro variables to SAS Job Execution Operator

### Fixed

- 

### Changed

- 

## [v0.0.6] - 2023-07-28

### Added

-

### Fixed

- macro vars blowing away env vars

### Changed

- 

## [v0.0.5] - 2023-07-14

### Deprecation Warning

Please switch from the SAS Studio Flow Operator to SAS Studio Operator. The SAS Studio Flow Operator will still function, but any new functionality will be added to the SAS Studio Operator.

### Added

- New operator SAS Studio Operator, superseding SAS Studio Flow Operator
- Ability to pass macro variables to the SAS Studio Operator
- Ability to get macro variables as output via xcom from SAS Studio Operator
- More templating parameters added
- Ability to execute programs instead of just flows added for the SAS Studio Operator
- Ability to pass in code to execute
- New operator to create a Compute session - SAS Compute Create Session Operator
- Ability to pass a compute session in to a SAS Studio Operator to avoid extra compute session startup time (optional)
- Ability to pass a compute session in to a SAS Job Execution Operator to avoid extra compute session startup time (optional)
- Note about security considerations in README
- New example example_studio_advanced.py

### Fixed

- Ability to pass in Airflow environment variables ([#12](https://github.com/sassoftware/sas-airflow-provider/issues/12))

### Changed

- Code refactoring and cleanup
- Existing examples have been updated

## [v0.0.4] - 2023-06-09

### Added

- Improved error handling for SAS Job Execution Operator
- util.py to encapsulate standard functionality shared by both operators - operators were updated accordingly

### Fixed

- SAS Job Execution Operator can now pull logs

### Changed

- 

## [v0.0.3] - 2023-05-26

### Added

- SAS Studio Flow Operator now handles cancled and timed out errors
- Airflow Exception handling to SAS Studio Flow Operator
- Templating support for SAS Studio Flow Operator
- Templating Support for SAS Job Execution Operator
- Example for templating (example_templating.py)

### Fixed

- 

### Changed

- Updated documentation (typos, clarification and NO_PROXY)
- example_sas_studioflow.py updated connection name
- HTTP Status Code handling for SAS Job Execution Operator

## [v0.0.2] - 2023-01-26

### Added

-

### Fixed

- 

### Changed

- exmaple_sas_studioflow.py to show connection support

## [v0.0.1] - 2023-01-12

Initial release
