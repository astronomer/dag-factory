# Why DAG Factory?

With AI coding agents, writing an Airflow DAG in Python is cheap. An agent can write a working DAG in a minute, and read
one just as fast. So "you don't have to write Python" is not a strong reason to use DAG Factory on its own.

The stronger reason is about what happens after a DAG is written. When an agent writes the code, writing is not the
expensive part. Reviewing it, checking it, and changing it later are. A team with many DAGs and many authors,
people or agents, still wants a workflow definition layer that is built for those jobs. AI sits upstream of DAG Factory,
not opposite it.

DAG Factory splits a DAG into two layers. The logic inside each task stays in Python. The workflow around it, meaning the
schedule, the tasks, the dependencies, and the retries, lives in YAML. That YAML layer is constrained, verifiable, and
maintainable, and each of these matters more as the number of DAGs grows.

## Constrained: a workflow does not need to be a program

A Python DAG file is a program, so it can do anything. It can create tasks in a loop, import helpers from anywhere, or
call an API at the top level while the file is being parsed. Airflow parses DAG files again and again, so slow top-level
code slows down the scheduler. Ask an agent for ten DAGs and you get ten slightly different styles, depending on the
prompt and the model. Each of these choices is something a reviewer has to notice.

A DAG Factory file leaves far fewer of these choices open. The schedule is always under `schedule`, the tasks are always
a list under `tasks`, and the dependencies are always a list on each task:

```yaml
etl:
  default_args:
    owner: data-team
    start_date: '2026-01-01'
  schedule: 0 3 * * *
  tasks:
    - task_id: extract
      operator: airflow.providers.standard.operators.bash.BashOperator
      bash_command: "python extract.py"
    - task_id: transform
      operator: airflow.providers.standard.operators.bash.BashOperator
      bash_command: "python transform.py"
      dependencies: [extract]
```

Mistakes still happen, but they happen inside a small, fixed structure. This is about structure, not safety: a task can
still run any command or use any importable Python object. What is fixed is where each part of the workflow is
described.

Python does not go away. The functions that clean data, call APIs, or train models stay in Python, and the YAML only
describes how they are scheduled and wired together. Other tools work the same way. dbt keeps the logic in SQL and the
model properties in YAML.

## Verifiable: checked before it reaches Airflow

YAML has a fixed grammar, so a DAG Factory config can be checked without running anything. A rule like "every DAG has an
owner", "no `BashOperator` in production", or "memory request under 4Gi" is a few lines of code over the YAML tree. The
same rule over Python DAGs needs AST analysis, and it still misses DAGs that are built dynamically.

AI makes writing code much faster, but not reviewing it, so review becomes the bottleneck. Every rule a machine can
check is review work that a person does not have to do.

It also lets an agent check its own work before any person looks at it. The agent writes the config, a validator checks
it, and the errors go back to the agent until the config is valid. This loop works best when errors use the terms of the
workflow, like "task `transform` depends on `extract_data`, which does not exist", rather than a stack trace from
Airflow's DAG parser.

[`dagfactory lint`](features/cli.md#lint-command) checks that each file is valid YAML and exits with an error if any
file is not, so it can gate a CI job or an agent's loop.

## Maintainable: keep the whole fleet up to date

Because every config has the same structure, a tool can change all of them at once.
[`dagfactory convert`](features/cli.md#convert-command) migrates YAML DAGs from Airflow 2 to Airflow 3. It renames
`schedule_interval` to `schedule` and moves operator import paths, such as `airflow.operators.bash.BashOperator`, to
their new `airflow.providers.standard` locations:

```bash
dagfactory convert dags/             # show the changes as a diff
dagfactory convert dags/ --override  # write them to the files
```

The same input always gives the same output, so the result is easy to review. The same migration over Python DAGs is not
reliably mechanical. Each Python file can build its DAG in its own way, so a tool cannot safely rewrite all of them. An
agent can do the migration, but it may change each file in a slightly different way, and each change has to be reviewed
like new code.

## When Python fits better

DAG Factory is not the right choice for every team. For a team with five DAGs, AI-written Python is fine, and YAML is one
more layer to learn. The choice depends mostly on how many pipelines you have, and how many people, or agents, write
them.

| DAG Factory fits better when                           | Python fits better when                                         |
| ------------------------------------------------------ | --------------------------------------------------------------- |
| You have many DAGs and many authors                    | You have a few DAGs and one small team                          |
| Most pipelines follow a few repeated patterns          | Each pipeline has its own unusual structure                     |
| You want policy checks before anything reaches Airflow | Reading the code in review is enough                            |
| You need changes across the whole fleet                | The DAG structure depends on complex logic or external systems  |

DAG Factory works best while the YAML stays small. When a workflow needs real logic to decide its own structure, write
that DAG in Python. Both kinds of DAG can run in the same Airflow deployment.

In a company with hundreds of DAGs, written by many teams and many agents, writing them is the cheap part. The hard part
is keeping all of them correct, consistent, and up to date, and that is where a constrained, verifiable, and
maintainable format helps the most.

To see the same workflow written both ways, see [A workflow definition, not a program](comparison/index.md).
