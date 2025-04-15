# Run Airflow3 Locally

This guide will walk you through the process of setting up Apache Airflow 3 locally using pip. You can choose either SQLite or Postgres as the database backend for Airflow.

## 1. Setup Postgres Container (Optional)

By default, SQLite will be used as Airflow metadata database unless you update the AIRFLOW__DATABASE__SQL_ALCHEMY_CONN environment variable to point to PostgreSQL. The following command will pull the official Postgres image , create a container named postgres, and expose the required ports.

### 1.1 Pull Postgres Image

```commandline
docker run --name postgres -p 5432:5432 -p 5433:5433 -e POSTGRES_PASSWORD=postgres postgres
```

### 1.2 Access the PostgreSQL Console and Create the Database

Now that the PostgreSQL container is running, you can connect to it via the command line using psql

```commandline
psql --u postgres
```

### 1.3 Create the Database for Airflow

Once you're inside the psql interactive terminal, you can create a new database that Airflow will use.

```commandline
CREATE DATABASE airflow_db;
```

## 2. Setup Virtual Environment for Airflow3

You need to configure the virtual environment for Airflow3.

### 2.1 Export ENV

This will export the AIRFLOW related env like AIRFLOW_HOME etc

```commandline
source scripts/airflow3/env.sh
```

## 3. Install Dependency

```commandline
sh scripts/airflow3/setup.sh
```

## 4. Run Airflow in Standalone Mode

Activate the virtual env created in previous step and run airflow

```commandline
source "$(pwd)/scripts/airflow3/venv-af3/bin/activate"

airflow standalone
```

This command will:

- Set the necessary environment variables (like AIRFLOW_HOME).
- Initialize the Airflow database.
- Start Airflow webserver, scheduler and trigger.

## 5. Run Airflow Tests

Once Airflow is running, you can also run tests.

```commandline
source scripts/airflow3/env.sh

source "$(pwd)/scripts/airflow3/venv-af3/bin/activate"

sh scripts/airflow3/tests.sh
```

## 6. Access the Airflow Web Interface

After running the standalone command, you can access the Airflow web interface to monitor the status of your DAGs, tasks, and more.

- The web interface should be available at [Localhost Server](http://localhost:8080)

## 7. Install Airflow from the Main Branch

If you want to install Airflow from the main branch, follow the steps from sections 1, 2, and 3 above. Then, proceed with the following steps:

### 7.1 Set ENV AIRFLOW_REPO_DIR

Set ENV `AIRFLOW_REPO_DIR` in scripts/airflow3/env.sh pointing to the path where your Airflow repository is cloned.

### 7.2 Activate the Virtual Environment

```commandline
source scripts/airflow3/env.sh

source "$(pwd)/scripts/airflow3/venv-af3/bin/activate"
```

### 7.3 Install Airflow from the Main Branch

```commandline
sh scripts/airflow3/install_from_main.sh
```

### 7.4 Run Airflow standalone

Finally, run Airflow in standalone mode again:

```commandline
airflow standalone
```
