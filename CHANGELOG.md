# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.15.0] - 2022-09-09
### Added
- Support for string concatenation of variables in YAML

## [0.14.0] - 2022-08-22
### Added
- Cast `on_retry_callback` from `default_args` to `Callable`

## [0.13.0] - 2022-05-27
### Added
- Add support for custom `timetable`
- Add support for `python_callable_file` for `PythonSensor` 

## [0.12.0] - 2022-02-07
### Added
- Allow `params` to be specified in YAML
- Add environment variables support for `python_callable_file`
- Adds support for `is_paused_upon_creation` flag
- Allow `python_callable` to be specified in YAML

## [0.11.1] - 2021-12-07
### Added
- Add support for `access_control` in DAG params 
### Fixed
- Fixed tests for Airflow 1.10 by pinning `wtforms`

## [0.11.0] - 2021-10-16
### Added
- Add support success/failure callables in `SqlSensor`
- Add `sla_secs` option in task param to convert seconds to timedelta object
### Fixed
- Support Airflow 2.2

## [0.10.1] - 2021-08-24
### Added
- Add support for `response_check_lambda` option in `HttpSensor`

## [0.10.0] - 2021-08-20
### Added
- Add support for `HttpSensor`

## [0.9.1] - 2021-07-27
### Added
- Add support for `python_callable_file` for `BranchPythonOperator`
### Fixed
- Only try to use `import_string` for callbacks if they are strings

## [0.9.0] - 2021-07-25
### Added
- Allow callbacks from Python modules

## [0.8.0] - 2021-06-09
### Added
- Support for `TaskGroups` if using Airflow 2.0
- Separate DAG building and registering logic 

## [0.7.2] - 2021-01-21
### Fixed
- Correctly set `dag.description` depending on Airflow version

## [0.7.1] - 2020-12-19
### Added
- Examples for using Custom Operator
### Fixed
- Handle `"None"` as `schedule_interval`

## [0.7.0] - 2020-12-19
### Added
- Support Airflow 2.0!

## [0.6.0] - 2020-11-16
### Added
- `catchup` added to DAG parameters
- Support for `ExternalTaskSensor`
- Run test suite against Python 3.8

## [0.5.0] - 2020-08-20
### Added
- Support for `KubernetesPodOperator`
- `doc_md` parameter at DAG level
- Import `doc_md` from a file or python callable
### Fixed
- `get_datetime` no longer removes time component

## [0.4.5] - 2020-06-17
### Fixed
- Do not include DAG `tags` parameter in Airflow versions that do not support it.
  
## [0.4.4] - 2020-06-12
### Fixed
- Use correct default for `tags` parameter
  
## [0.4.3] - 2020-05-24
### Added
- `execution_timeout` parse at task level
- `tags` parameter at DAG level

## [0.4.2] - 2020-03-28
### Added
- Method `clean_dags` to clean old dags that might not exist anymore
### Changed
- `airflow` version

## [0.4.1] - 2020-02-18
### Fixed
- Default `default_view` parameter to value from `airflow.cfg`
  
## [0.4.0] - 2020-02-12
### Added
- Support for additional DAG parameters
### Fixed
- Define Loader when loading YAML file

## [0.3.0] - 2019-10-11
### Added
- Support for PythonOperator tasks
### Changed
- Cleaned up testing suite and added pylint to builds

## [0.2.2] - 2019-09-08
### Changed
- `airflow` version
### Removed
- `piplock` and `pipfile` files
  
## [0.2.1] - 2019-02-26
### Added
- Python 3+ type-annotations

## [0.2.0] - 2018-11-28
### Added
- Added badges to README
- Support for timezone aware DAGs
- This CHANGELOG!

## [0.1.1] - 2018-11-20
### Removed
- Removed `logme` dependency 

## [0.1.0] - 2018-11-20
- Initial release

[Unreleased]: https://github.com/ajbosco/dag-factory/compare/v0.15.0...HEAD
[0.15.0]: https://github.com/ajbosco/dag-factory/compare/v0.14.0...v0.15.0
[0.14.0]: https://github.com/ajbosco/dag-factory/compare/v0.13.0...v0.14.0
[0.13.0]: https://github.com/ajbosco/dag-factory/compare/v0.12.0...v0.13.0
[0.12.0]: https://github.com/ajbosco/dag-factory/compare/v0.11.1...v0.12.0
[0.11.1]: https://github.com/ajbosco/dag-factory/compare/v0.11.0...v0.11.1
[0.11.0]: https://github.com/ajbosco/dag-factory/compare/v0.10.1...v0.11.0
[0.10.1]: https://github.com/ajbosco/dag-factory/compare/v0.10.0...v0.10.1
[0.10.0]: https://github.com/ajbosco/dag-factory/compare/v0.9.1...v0.10.0
[0.9.1]: https://github.com/ajbosco/dag-factory/compare/v0.9.0...v0.9.1
[0.9.0]: https://github.com/ajbosco/dag-factory/compare/v0.8.0...v0.9.0
[0.8.0]: https://github.com/ajbosco/dag-factory/compare/v0.7.2...v0.8.0
[0.7.2]: https://github.com/ajbosco/dag-factory/compare/v0.7.1...v0.7.2
[0.7.1]: https://github.com/ajbosco/dag-factory/compare/v0.7.0...v0.7.1
[0.7.0]: https://github.com/ajbosco/dag-factory/compare/v0.6.0...v0.7.0
[0.6.0]: https://github.com/ajbosco/dag-factory/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/ajbosco/dag-factory/compare/v0.4.5...v0.5.0
[0.4.5]: https://github.com/ajbosco/dag-factory/compare/v0.4.4...v0.4.5
[0.4.4]: https://github.com/ajbosco/dag-factory/compare/v0.4.3...v0.4.4
[0.4.3]: https://github.com/ajbosco/dag-factory/compare/v0.4.2...v0.4.3
[0.4.2]: https://github.com/ajbosco/dag-factory/compare/v0.4.1...v0.4.2
[0.4.1]: https://github.com/ajbosco/dag-factory/compare/v0.4.0...v0.4.1
[0.4.0]: https://github.com/ajbosco/dag-factory/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/ajbosco/dag-factory/compare/v0.2.2...v0.3.0
[0.2.2]: https://github.com/ajbosco/dag-factory/compare/v0.2.0...v0.2.2
[0.2.1]: https://github.com/ajbosco/dag-factory/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/ajbosco/dag-factory/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/ajbosco/dag-factory/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/ajbosco/dag-factory/releases/tag/v0.1.0
