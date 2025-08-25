<!-- markdownlint-disable MD007 -->
# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0a2] - 2025-08-01

### Breaking Changes

- Airflow providers are now optional dependencies by @pankajastro in [#486](https://github.com/astronomer/dag-factory/pull/486)
  - Previously, `dag-factory` enforced the installation of `apache-airflow-providers-http` and `apache-airflow-providers-cncf-kubernetes`. These Airflow providers dependencies are now optional. If your DAGs depend on these providers, you must install them manually. Alternatively, you can install `dag-factory` with extras like `dag-factory[all]`, `dag-factory[kubernetes]`, etc.
- Removed `clean_dags` function by @pankajastro in [#498](https://github.com/astronomer/dag-factory/pull/498)
  - You no longer need to call `example_dag_factory.clean_dags(globals())` in your DAG files. DAG cleanup is now controlled via the Airflow config setting `AIRFLOW__DAG_PROCESSOR__REFRESH_INTERVAL`.
- Remove `schedule_interval` parameter from DAG configuration YAML by @viiccwen in [#503](https://github.com/astronomer/dag-factory/pull/503)
  - Use `schedule` parameter instead of `schedule_interval`.
- Change `DagFactory` class access to private by @pankajastro in [#509](https://github.com/astronomer/dag-factory/pull/509)
  - The import path `from dagfactory import DagFactory` has been removed.
  - The class `DagFactory` has been renamed to `_DagFactory`.
  - The `generate_dags` method of `DagFactory` has been renamed to `_generate_dags`.
- Remove Inconsistent Parameters for Airflow Consistent by @pankajastro in [#512](https://github.com/astronomer/dag-factory/pull/512)
  - Removed `dagrun_timeout_sec` from dag param.
  - Removed `retry_delay_sec`, `sla_secs` from default_args.
  - Removed accepting `execution_timeout` as integer.
  - Removed `execution_timeout_secs`, `sla_secs` and `execution_delta_secs` from task param.
- Remove custom parsing for Kubernetes object and refactor KPO to use `__type__` syntax by @pankajastro in [#523](https://github.com/astronomer/dag-factory/pull/523)
  - The custom parsing for Kubernetes objects has been removed. You can no longer pass a custom YAML dictionary to DAG-Factory configuration unless accepted by the KubernetesPodOperator. We suggest you to use `__type__` syntax to supply Kubernetes object in your YAML DAG. For an example KPO configuration, visit: [KubernetesPodOperator Documentation](https://astronomer.github.io/dag-factory/dev/features/kpo/).
- Consolidate `!and`, `!or`, `!join`, `and` and `or` key in YAML DAG configuration by @pankajastro in [#525](https://github.com/astronomer/dag-factory/pull/525)
  - Use `__and__`, `__or__` and `join__` instead
- Remove custom parsing for DAG parameter `timetable` by @pankajastro in [#533](https://github.com/astronomer/dag-factory/pull/533)
  - Use the `__type__` annotation for the `timetable` parameter.
- Rename parameter of `load_yaml_dags` and `_DagFactory` to reflect behaviour by @pankajastro in [#546](https://github.com/astronomer/dag-factory/pull/546)
  - Rename `config` to `config_dict`
  - Rename `default_args_config_path` to `defaults_config_path`
  - Rename `default_args_config_dict`  to `defaults_config_dict`

### Added

- Support dag-level arguments in global defaults by @gyli in [#480](https://github.com/astronomer/dag-factory/pull/480)
- Support `*args` in custom Python object by @pankajastro in [#484](https://github.com/astronomer/dag-factory/pull/484)
- Support tasks and task_groups as lists by @pankajkoti in [#487](https://github.com/astronomer/dag-factory/pull/487)
- Support overriding `defaults.yml` based on the directory hierarchy by @tatiana in [#500](https://github.com/astronomer/dag-factory/pull/500)
- Introduced DAG Factory CLI by @tatiana in [#510](https://github.com/astronomer/dag-factory/pull/510)
- Added `lint` command to CLI by @tatiana in [#513](https://github.com/astronomer/dag-factory/pull/513)
- Add convert CLI command to migrate from af2 to af3 by @tatiana in [539](https://github.com/astronomer/dag-factory/pull/539)

### Fixed

- Fix the Airflow version condition check to parse inlets/outlets syntax according to the dataset by @pankajastro in [#485](https://github.com/astronomer/dag-factory/pull/485)
- Ensure `dag_params` contain `schedule` before operating on it by @pankajkoti in [#488](https://github.com/astronomer/dag-factory/pull/488)
- Fix `start_date`, `end_date` at the DAG level by @pankajastro in [#495](https://github.com/astronomer/dag-factory/pull/495)
- Allow `execution_timeout` in `default_args` by @pankajastro in [#501](https://github.com/astronomer/dag-factory/pull/501)
- Capture Telemetry DNS gaierror and handle it gracefully by @tatiana in [#544](https://github.com/astronomer/dag-factory/pull/544)

### Docs

- Restore basic DAG example by @pankajastro in [#483](https://github.com/astronomer/dag-factory/pull/483)
- Replace the usages in example dags, tests and docs for tasks and taskgroups to be list by @pankajkoti in [#492](https://github.com/astronomer/dag-factory/pull/492)
- Update default documentation based on #500 by @tatiana in [#504](https://github.com/astronomer/dag-factory/pull/504)
- Add more examples for Custom Python object by @pankajastro in [#506](https://github.com/astronomer/dag-factory/pull/506)
- Add documentation for DAG Factory CLI by @tatiana in [#511](https://github.com/astronomer/dag-factory/pull/511)
- Add documentation and example YAMLs for task and task_group configuration formats by @pankajkoti in [#530](https://github.com/astronomer/dag-factory/pull/530)
- Add migration guide docs by @pankajastro in [#532](https://github.com/astronomer/dag-factory/pull/532)
- Docs: Fix rendering of note block by @pankajastro in [#537](https://github.com/astronomer/dag-factory/pull/537)
- Document the Asset example DAG by @pankajastro in [#538](https://github.com/astronomer/dag-factory/pull/538)
- Add docs for the CLI convert command by @tatiana in [#541](https://github.com/astronomer/dag-factory/pull/541)
- Add remaining breaking changes in migration guide by @pankajastro in [#549](https://github.com/astronomer/dag-factory/pull/549)
- Fix typos in scheduling and datasets docs by @viiccwen in [#565](https://github.com/astronomer/dag-factory/pull/565)
- docs: Make markdownlint happy (fix MD007 ul-indent in dev/README.md) by @viiccwen in [#566](https://github.com/astronomer/dag-factory/pull/566)

### Other Changes

- Improve unit tests to disregard `$AIRFLOW_HOME` by @tatiana in [#490](https://github.com/astronomer/dag-factory/pull/490)
- Resolve unpinned action reference error alerts raised by Zizmor by @pankajkoti in [#493](https://github.com/astronomer/dag-factory/pull/493)
- Resolve 'credential persistence through GitHub Actions artifacts' warnings from Zizmor by @pankajkoti in [#494](https://github.com/astronomer/dag-factory/pull/494)
- Resolve 'overly broad permissions' warnings from Zizmor by @pankajkoti in [#496](https://github.com/astronomer/dag-factory/pull/496)
- CI: Add GitHub CodeQL analysis workflow (`codeql.yml`) by @pankajkoti in [#497](https://github.com/astronomer/dag-factory/pull/497)
- Fix deploy pages job missing credentials by @pankajkoti in [#499](https://github.com/astronomer/dag-factory/pull/499)
- Add the breaking changes to changelog by @pankajastro in [#502](https://github.com/astronomer/dag-factory/pull/502)
- Add pre-commit to update `uv.lock` by @pankajastro in [#514](https://github.com/astronomer/dag-factory/pull/514)
- Remove `clean_dags` usage from object storage DAG by @pankajastro in [#515](https://github.com/astronomer/dag-factory/pull/515)
- Remove broad exceptions and catch more specific exceptions by @pankajastro in [#519](https://github.com/astronomer/dag-factory/pull/519)
- Add missing env in contributing doc by @pankajastro in [#522](https://github.com/astronomer/dag-factory/pull/522)
- Enhance PyPI Stats API error handling by @viiccwen in [#535](https://github.com/astronomer/dag-factory/pull/535)
- Update example to be Airflow 3 compatible by @tatiana in [540](https://github.com/astronomer/dag-factory/pull/540)
- Remove AUTO_CONVERT_TO_AF3 from tests by @tatiana in [#543](https://github.com/astronomer/dag-factory/pull/543)
- Bump actions/download-artifact from 4 to 5 by @dependabot in [#548](https://github.com/astronomer/dag-factory/pull/548), [#558](https://github.com/astronomer/dag-factory/pull/558), [#559](https://github.com/astronomer/dag-factory/pull/559), [#560](https://github.com/astronomer/dag-factory/pull/560), [#562](https://github.com/astronomer/dag-factory/pull/562) and [#653](https://github.com/astronomer/dag-factory/pull/563)
- Update pyproject.toml to Sync Test Versions with CI/CD Pipeline by @viiccwen in [#553](https://github.com/astronomer/dag-factory/pull/553)
- Fix file URI format in ObjectStoragePath example to prevent duplicate slashes by @viiccwen in [#556](https://github.com/astronomer/dag-factory/pull/556)
- CI: Only build docs on PR push event and deploy for merge and release by @pankajastro in [#568](https://github.com/astronomer/dag-factory/pull/568)

## [0.23.0] - 2025-07-14

### Breaking Change

- Drop Airflow 2.2 Support by @pankajastro in [#388](https://github.com/astronomer/dag-factory/pull/388)
- Drop Python 3.8 support by @pankajastro in [#435](https://github.com/astronomer/dag-factory/pull/435)
- Drop Airflow 2.3 Support by @pankajastro in [#456](https://github.com/astronomer/dag-factory/pull/456)

### Airflow 3 Support

- Add tools for Dag-factory Airflow3 testing by @pankajastro in [#395](https://github.com/astronomer/dag-factory/pull/395)
- Fix schedule args for Airflow 3 by @pankajastro in [#413](https://github.com/astronomer/dag-factory/pull/413)
- Fix import path for BranchPythonOperator, PythonOperator and PythonSensor by @pankajastro in [#414](https://github.com/astronomer/dag-factory/pull/414)
- Add scheduling docs for Airflow 3 by @pankajastro in [#424](https://github.com/astronomer/dag-factory/pull/424)
- Enable Airflow 3 tests in CI by @pankajastro in [#436](https://github.com/astronomer/dag-factory/pull/436)
- Add env AUTO_CONVERT_TO_AF3 in Dockerfile by @pankajastro in [#455](https://github.com/astronomer/dag-factory/pull/455)
- Validate DAG's on Airflow 3 by @pankajastro in [#457](https://github.com/astronomer/dag-factory/pull/457)
- Refactor schedule to use the Python object @pankajastro in [#458](https://github.com/astronomer/dag-factory/pull/458)
- Fix CI and import issues for Airflow 3 compatibility @pankajastro in [#463](https://github.com/astronomer/dag-factory/pull/463)

### Added

- Add support for defining inlets by @IvanSviridov in [#380](https://github.com/astronomer/dag-factory/pull/380)
- Add HttpOperator JSON serialization support with tests by @a-chumagin in [#382](https://github.com/astronomer/dag-factory/pull/382)
- Add support for custom Python object by @pankajastro in [#444](https://github.com/astronomer/dag-factory/pull/444)
- Support env var in default by @gyli in [#452](https://github.com/astronomer/dag-factory/pull/452)
- Pass default arguments via dictionary in .py file by @jroachgolf84 in [#465](https://github.com/astronomer/dag-factory/pull/465)

### Fixed

- Remediated ``default`` behavior, added documentation by @jroachgolf84 in [#378](https://github.com/astronomer/dag-factory/pull/378)
- Upgrade `apache-airflow-providers-cncf-kubernetes` provider by @pankajastro in [#407](https://github.com/astronomer/dag-factory/pull/407)
- Include error message trace in exception by @pankajastro in [#408](https://github.com/astronomer/dag-factory/pull/408)

### Docs

- Remove Unreleased heading section from the CHANGELOG.md by @pankajkoti in [#365](https://github.com/astronomer/dag-factory/pull/365)
- Add Documentation for Conditional Dataset Scheduling with dag-factory by @ErickSeo in [#367](https://github.com/astronomer/dag-factory/pull/367)
- Add copy right in docs footer by @pankajastro in [#371](https://github.com/astronomer/dag-factory/pull/371)
- Updating docs for callbacks by @jroachgolf84 in [#375](https://github.com/astronomer/dag-factory/pull/375)
- Add stable/latest version in docs by @pankajastro in [#391](https://github.com/astronomer/dag-factory/pull/391)
- Migrate old content to new documentation structure by @pankajastro in [#393](https://github.com/astronomer/dag-factory/pull/393)
- Update Airflow supported version 2.3+ in docs by @pankajastro in [#412](https://github.com/astronomer/dag-factory/pull/412)
- Doc: Add step to fork repo in contributing guide  by @pankajastro in [#427](https://github.com/astronomer/dag-factory/pull/427)
- Add setting CONFIG_ROOT_DIR in the contribution doc by @gyli in [#432](https://github.com/astronomer/dag-factory/pull/432)
- Add DAG example showcasing runtime params usage by @pankajastro in [#449](https://github.com/astronomer/dag-factory/pull/449)
- Add jinja2 template example by @pankajastro in [#450](https://github.com/astronomer/dag-factory/pull/450)

### Others

- Add Scraf Pixels for telemetry by @pankajastro in [#373](https://github.com/astronomer/dag-factory/pull/373)
- feat: bumped http provider versions to 2.0+ by @a-chumagin in [#389](https://github.com/astronomer/dag-factory/pull/389)
- Add --verbosity debug in astro-cli cmd by @pankajastro in [#390](https://github.com/astronomer/dag-factory/pull/390)
- Add missing Python file for dynamic task example by @pankajastro in [#392](https://github.com/astronomer/dag-factory/pull/392)
- Pin apache-airflow-providers-cncf-kubernetes<10.4.2 by @pankajastro in [#400](https://github.com/astronomer/dag-factory/pull/400)
- Add script to check version and tag by @pankajastro in [#395](https://github.com/astronomer/dag-factory/pull/394)
- Assert DagRunState in integration test by @pankajastro in [#415](https://github.com/astronomer/dag-factory/pull/415)
- Move to uv for package management by @jlaneve in [#419](https://github.com/astronomer/dag-factory/pull/419)
- Install uv in CI by @jlaneve in [#421](https://github.com/astronomer/dag-factory/pull/421)
- Bump astral-sh/setup-uv from 5 to 6 by @dependabot in [#423](https://github.com/astronomer/dag-factory/pull/423)
- Add Airflow 2.11 in test matrix by @pankajastro in [#425](https://github.com/astronomer/dag-factory/pull/425)
- fix example_dag_factory.yml typo causing catchup: false to not be respected by @RNHTTR in [#431](https://github.com/astronomer/dag-factory/pull/431)
- Remove expandvars in utils.get_python_callable by @gyli in [#440](https://github.com/astronomer/dag-factory/pull/440)
- Delete unused img folder by @pankajastro in [#446](https://github.com/astronomer/dag-factory/pull/446)
- Clean print statement by @pankajastro [#447](https://github.com/astronomer/dag-factory/pull/447)
- Add hatch to uv dev dependencies by @gyli in [#453](https://github.com/astronomer/dag-factory/pull/453)
- Add Authorize Job in CI by @pankajastro in [#460](https://github.com/astronomer/dag-factory/pull/460)
- Remove PyPI token for releasing packages by @tatiana in [#461](https://github.com/astronomer/dag-factory/pull/461)
- Change CI on trigger event to pull_request from pull_request_target by @pankajkoti in [#464](https://github.com/astronomer/dag-factory/pull/464)
- Update cicd.yaml: Use pull_request for authorize as we don't have pull_request_target event configured by @pankajkoti in [#466](https://github.com/astronomer/dag-factory/pull/466)
- Add environment for pypi publish job by @pankajastro in [#478](https://github.com/astronomer/dag-factory/pull/478)

## [0.22.0] - 2025-01-10

### Added

- Propagate provided dag_display_name to built dag by @pankajkoti in #326
- Add incipient documentation tooling by @tatiana in #328
- Support loading `default_args` from shared `defaults.yml` by @pankajastro in #330
- Add security policy by @tatiana in #339
- Add Robust Support for Callbacks at Task and TaskGroup Level by @@jroach-astronomer in #322
- Support `ExternalTaskSensor` `execution_date_fn` and `execution_delta` by @tatiana in #354
- Refactor and add support for schedule conditions in DAG configuration by @ErickSeo in #320

### Fixed

- Handle gracefully exceptions during telemetry collection by @tatiana in #335
- Adjust `markdownlint` configuration to enforce 4-space indentation for proper `mkdocs` rendering by @pankajkoti in #345

### Docs

- Create initial documentation index by @tatiana in #325
- Use absolute URLs for failing links in docs/index.md by @pankajkoti in #331
- Add quick start docs by @pankajastro in #324
- Add docs comparing Python and YAML-based DAGs by @tatiana in #327
- Add docs about project contributors and their roles by @tatiana in #341
- Add documentation to support developers by @tatiana in #343
- Add docs for configuring workflows, environment variables and defaults by @pankajkoti in #338
- Add code of conduct for contributors and DAG factory community by @tatiana in #340
- Document Dynamic Task Mapping feature by @pankajkoti in #344
- Fix warning message 404 in code_of_conduct docs by @pankajastro in #346
- Update theme for documentation by @pankajastro in #348
- Fix markdownlint errors and some rendering improvements by @pankajastro in #356
- Reword content in documentation by @yanmastin-astro in #336

### Others

- Improve integration tests scripts by @tatiana in #316
- Add Markdown pre-commit checks by @tatiana in #329
- Remove Airflow <> 2.0.0 check by @pankajastro in #334
- Reduce telemetry timeout from 5 to 1 second by @tatiana in #337
- Add GH action job to deploy docs by @pankajastro in #342
- Enable Depandabot to scan outdated Github Actions dependencies by @tatiana in #347
- Improve docs deploy job by @pankajastro in #352
- Unify how we build dagfactory by @tatiana in #353
- Fix running make docker run when previous versions were run locally by @tatiana in #362
- Install `jq` in `dev` container by @pankajastro in #363
- Dependabot GitHub actions version upgrades in #349, #350, #351

## [0.21.0] - 2024-12-06

### Added

- Support Task Flow and enhance dynamic task mapping by @tatiana in #314
- Render YML DAG config as DAG Docs by @pankajastro #305
- Support building DAGs from topologically unsorted YAML files by @tatiana in #307
- Add support for nested task groups by @glazunov996 and @pankajastro in #292
- Add support for templating `on_failure_callback` by @jroach-astronomer #252

### Fixed

- Fix compatibility with apache-airflow-providers-cncf-kubernetes>=10.0.0 by @tatiana in #311
- Refactor telemetry to collect events during DAG run and not during DAG parsing by @pankajastro #300

### Docs

- Fix reference for HttpSensor in README.md by @pankajastro in #277
- Add example DAG for task group by @pankajastro in #293
- Add CODEOWNERS by @pankajkoti in #270
- Update CODEOWNERS to track all files by @pankajkoti in #276
- Modified Status badge in README by @jaejun #298

### Others

- Add GitHub issue template for bug reports and feature requests by @pankajkoti in #269
- Refactor dynamic task mapping implementation by @tatiana in #313
- Remove pytest durations from tests by @tatiana in #309
- Remove DAG retries check since many DAGs have different retry values by @tatiana in #310
- Lint fixes after running  `pre-commit run --all-files` by @tatiana in #312
- Remove redundant exception code by @pankajastro #294

## [0.20.0] - 2024-10-22

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

- Build DAGs when there is an invalid YAML in the DAGs folder by @quydx and @tatiana in #184

### Others

- Development tools
  - Fix make docker-run by @pankajkoti in #249
  - Add vim dot files to .gitignore by @tatiana in #228
  - Add local airflow setup files to .gitignore by @pankajkoti in #246
  - Use Hatchling to modern package building by @kaxil in #208
  - Cleanup dependabot, MANIFEST.in and Makefile by @pankajastro in #268
  - Add Astro CLI project to validate DAG Factory by @pankajastro in #267
  - Fix Makefile to run make docker-run by @tatiana in #271
- CI
  - Fix static check failures in PR #218 by @pankajkoti in #251
  - Fix pre-commit checks by @tatiana in #247
  - Remove tox and corresponding build jobs in CI by @pankajkoti in #248
  - Install Airflow with different versions in the CI by @pankajkoti in #237
  - Run pre-commit hooks on all existing files by @pankajkoti in #245
  - Add Python 3.11 and 3.12 to CI test pipeline by @pankajkoti in #229
  - Fix release action and overall CI jobs dependencies by @tatiana in #261
- Packaging & Release
  - Configure GitHub to automate publishing DAG Factory in PyPI by @tatiana in #255
  - Update pyproject classifiers for Python 3.11 and 3.12 by @pankajastro in #262
  - Update http sensor example to Airflow 2.0 by @pankajastro in #265
- Tests
  - Fix duplicate test name by @pankajastro in #234
  - Add static check by @pankajastro in #231
  - Fix running tests locally (outside the CI) by @tatiana in #227
  - Add the task_2 back to dataset example by @cmarteepants in #204
  - Remove unnecessary config line by @jlaneve in #202
  - Fix Pytest fixture that changed DAG YAML file by @tatiana in #256
  - Run integration tests in CI by @pankajkoti in #266
  - Improve test coverage by @pankajastro in #258
- Refactor
  - Refactor poor exception handling by @tatiana in #259
  - Remove off looking start-date value in example_dag yaml config by @pankajkoti in #273
- Documentation
  - Update the license from MIT to Apache 2.0 by @pankajastro in #191
  - Add registration icon and links to Airflow references by @cmarteepants in #190
  - Update quickstart and add feature examples by @cmarteepants #189
  - Fix `README.md` badges by @tatiana in #260
  - Remove duplicated operator in `README.md` by @brair in #263

### Breaking changes

- Removed support for Python 3.7
- The license was changed from MIT to Apache 2.0

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
