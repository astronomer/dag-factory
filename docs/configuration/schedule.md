# Scheduling

- *Released in version: 0.23.0*

DAG-Factory offers flexible scheduling options so your DAGs can run on time or based on data availability. Whether you're triggering DAGs on a cron schedule, a time delta, or when an upstream asset is ready, you can configure it easily using the schedule field in your YAML.

Below are the supported scheduling types, each with consistent structure and examples to help you get started.

## How to Use

- Every schedule block must specify a type, such as `cron`, `timedelta`, `relativedelta`, `timetable`, or `assets`.
- The actual configuration goes under the value key.
- Only one schedule type should be defined per DAG.

## Example Overview

| Type            | Description                           | Use Case Example                    |
|-----------------|---------------------------------------|-------------------------------------|
| `cron`          | Run based on a cron string            | Every day at midnight               |
| `timedelta`     | Fixed intervals between runs          | Every 6 hours                       |
| `relativedelta` | Calendar-aware schedule (e.g. months) | Every 1st of the month              |
| `timetable`     | Advanced Airflow timetables           | Custom trigger logic                |
| `assets`        | Trigger based on asset readiness      | When data `X` and `Y` are available |
| `datasets`      | Trigger based on datasets readiness   | When data `X` and `Y` are available |

## Schema Options

### 1. Cron-Based Schedule

```yaml title="Corn Schedule"
--8<-- "tests/schedule/cron.yml"
```

Or,

```yaml title="Corn Schedule"
--8<-- "tests/schedule/cron_dict.yml"
```

### 2. Timedelta Schedule

```yaml title="Timedelta Schedule"
--8<-- "tests/schedule/timedelta.yml"
```

### 3. RelativeDelta Schedule

```yaml title="Relativedelta Schedule"
--8<-- "tests/schedule/relativedelta.yml"
```

## 4. Timetable (Advanced Scheduling)

```yaml title="Timetable Schedule"
--8<-- "tests/schedule/timetable.yml"
```

### 5. Asset-Based Triggering

#### OR (default when list is provided)

```yaml title="OR Condition"
--8<-- "tests/schedule/list_asset.yml"
```

```yaml title="OR Condition"
--8<-- "tests/schedule/or_asset.yml"
```

#### AND (explicit composition)

```yaml title="AND Condition"
--8<-- "tests/schedule/and_asset.yml"
```

#### Nested And Or Condition

```yaml title="Nested AND OR Condition"
--8<-- "tests/schedule/nested_asset.yml"
```

#### With Watchers

```yaml title="Assert with watcher"
--8<-- "tests/schedule/asset_with_watcher.yml"
```

### 6. Datasets-Based Triggering

```yaml
schedule: [ 's3://bucket_example/raw/dataset1.json', 's3://bucket_example/raw/dataset2.json' ]
```
