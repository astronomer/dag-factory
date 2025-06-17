# Scheduling in DAG-Factory

DAG-Factory offers flexible scheduling options so your DAGs can run on time or based on data availability. Whether you're triggering DAGs on a cron schedule, a time delta, or when an upstream asset is ready, you can configure it easily using the schedule field in your YAML.

Below are the supported scheduling types, each with consistent structure and examples to help you get started.

## How to Use

- Every schedule block must specify a type, such as `cron`, `timedelta`, `relativedelta`, `timetable`, or `assets`.
- The actual configuration goes under the value key.
- Only one schedule type should be defined per DAG.

## Example Overview

| Type            | Description                           | Use Case Example                    |
| --------------- | ------------------------------------- |-------------------------------------|
| `cron`          | Run based on a cron string            | Every day at midnight               |
| `timedelta`     | Fixed intervals between runs          | Every 6 hours                       |
| `relativedelta` | Calendar-aware schedule (e.g. months) | Every 1st of the month              |
| `timetable`     | Advanced Airflow timetables           | Custom trigger logic                |
| `assets`        | Trigger based on asset readiness      | When data `X` and `Y` are available |

## Schema Options

### 1. Cron-Based Schedule

```yaml
schedule:
  type: cron
  value: "0 0 * * *"
```

### 2. Timedelta Schedule

```yaml
schedule:
  type: timedelta
  value:
    days: 0
    seconds: 0
    microseconds: 0
    milliseconds: 0
    minutes: 0
    hours: 6
    weeks: 0
```

### 3. RelativeDelta Schedule

```yaml
schedule:
  type: relativedelta
  value:
    months: 1
    days: 0
```

## 4. Timetable (Advanced Scheduling)

```yaml
schedule:
  type: timetable
  value:
    callable: airflow.timetables.trigger.CronTriggerTimetable
    params:
      cron: "0 1 * * 3"
      timezone: "UTC"

```

### 5. Asset-Based Triggering

#### OR (default when list is provided)

```yaml
schedule:
  - uri: s3://dag1/output_1.txt
    extra:
      hi: bye
  - uri: s3://dag2/output_1.txt
    extra:
      hi: bye
```

#### AND (explicit composition)

```yaml
schedule:
  type: assets
  value:
    and:
      - uri: s3://dag1/output_1.txt
        extra:
          hi: bye
      - uri: s3://dag2/output_1.txt
        extra:
          hi: bye
```

#### With Watchers

```yaml
schedule:
  type: assets
  value:
    - uri: s3://dag1/output_1.txt
      extra:
        hi: bye
      watchers:
        - class: airflow.sdk.AssetWatcher
          name: test_asset_watcher
          trigger:
            class: airflow.providers.standard.triggers.file.FileDeleteTrigger
            params:
              filepath: "/temp/file.txt"
```
