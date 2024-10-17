# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
### Changed
- Removed support for Python 3.7

## [0.20.0a1] - 2024-08-17

### Added
- Support using envvar in config YAML by @tatiana in #236
- **Callback improvements**
  - Support installed code via python callable string by @john-drews in #221
  - Add `callback_file` & `callback_name` to `default_args` DAG level by @subbota19 in #218
  - Cast callbacks to functions when set with `default_args` on TaskGroups by @Baraldo and @pankajastro in #235

- **Telemetry**
  - For more information, please, read the [Privacy Notice](https://github.com/astronomer/dag-factory/blob/main/PRIVACY_NOTICE.md#collection-of-data).
  - Add scarf to readme for website analytics by @cmarteepants in #219
  - Support telemetry during DAG parsing emitting data to Scarf by @tatiana in #250.

### Fixed
- Build DAGs when tehre is an invalid YAML in the DAGs folder by @quydx and @tatiana in #184

### Others
- Fix static check failures in PR #218 by @pankajkoti in #251
- Fix make docker-run by @pankajkoti in #249
- Fix pre-commit checks by @tatiana in #247
- Remove tox and corresponding build jobs in CI by @pankajkoti in #248
- Install Airflow with different versions in the CI by @pankajkoti in #237
- Run pre-commit hooks on all existing files by @pankajkoti in #245
- Fix duplicate test name by @pankajastro in #234
- Add static check by @pankajastro in #231
- Add Python 3.11 and 3.12 to CI test pipeline by @pankajkoti in #229
- Add vim dot files to .gitignore by @tatiana in #228
- Fix running tests locally (outside the CI) by @tatiana in #227
- Use Hatchling to modern package building by @kaxil in #208
- Add the task_2 back to dataset example by @cmarteepants in #204
- Remove unnecessary config line by @jlaneve in #202
- Update the license from MIT to Apache 2.0 by @pankajastro in #191
- Add registration icon and links to Airflow references by @cmarteepants in #190
- Update quickstart and add feature examples by @cmarteepants #189


## [0.19.0] - 2023-07-19
### Added
- Support for Airflow Datasets (data-aware scheduling)
- Support for inherited Operators

## [0.18.1] - 2023-04-28
### Fixed
- Set default value for `render_template_as_native_obj` to False

## [0.18.0] - 2023-04-11
### Added
- Support for dynamic task mapping
- Support for `render_template_as_native_obj`
- Support for `template_searchpath`

## [0.17.3] - 2023-03-27
### Added
- dag-factory specific `Exceptions` with more meaningful names
### Fixed
- Reverts allowing inheritance of `KubernetesPodOperator`
- Now passing CI for lint check

## [0.17.2] - 2023-03-26
### Changed
- Allow inheritance of `KubernetesPodOperator`

## [0.17.1] - 2023-01-21
### Changed
- Changed imports to support Kubernetes Provider > 5.0.0

## [0.17.0] - 2022-12-06
### Added
- Adds `sla_secs` in `default_args` to convert seconds to `timedelta` obj
### Changed
- Changed deprecated imports to support Airflow 2.5
- Removed support for Python 3.6

## [0.16.0] - 2022-11-13
### Added
- Function to scan recursively for YAML DAGs
### Changed
- Changed deprecated imports to support Airflow 2.4+

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

[Unreleased]: https://github.com/ajbosco/dag-factory/compare/v0.19.0...HEAD
[0.19.0]: https://github.com/ajbosco/dag-factory/compare/v0.18.1...v0.19.0
[0.18.1]: https://github.com/ajbosco/dag-factory/compare/v0.18.0...v0.18.1
[0.18.0]: https://github.com/ajbosco/dag-factory/compare/v0.17.3...v0.18.0
[0.17.3]: https://github.com/ajbosco/dag-factory/compare/v0.17.2...v0.17.3
[0.17.2]: https://github.com/ajbosco/dag-factory/compare/v0.17.1...v0.17.2
[0.17.1]: https://github.com/ajbosco/dag-factory/compare/v0.17.0...v0.17.1
[0.17.0]: https://github.com/ajbosco/dag-factory/compare/v0.16.0...v0.17.0
[0.16.0]: https://github.com/ajbosco/dag-factory/compare/v0.15.0...v0.16.0
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
