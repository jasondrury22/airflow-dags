# Local Development Environment (Airflow)

This project includes a custom, fast local development sandbox. It is designed to let contributors test DAG parsing, UI interactions, and core Airflow logic without needing to download gigabytes of heavy machine learning dependencies or worry about accidental cloud interactions.

## Architecture Highlights

* **Ultra-Lightweight:** Bypasses heavy C++ compilations and large ML libraries (like `xarray`, `zarr`, `pvnet`, etc.) by dynamically injecting `MagicMock` objects via a custom `airflow_local_settings.py`.
* **Air-Gapped Security (DMZ):** Uses a demilitarized Docker network architecture. The Scheduler is completely isolated from the internet to prevent accidental API calls or data leaks, while the Webserver bridges to `localhost` to serve the UI safely.
* **Automated Provisioning:** A single startup script handles the database initialization, runs migrations, and provisions the default admin user automatically.
* **Resource Optimized:** Extends Gunicorn worker timeouts and patches `gevent` to prevent the memory leaks and CPU choking common in local Airflow deployments.

## Quick Start

**Prerequisites:** Ensure you have Docker and Docker Compose installed and running.

### 1. Clean the Slate
If you have run previous versions of this environment, clear out the old SQLite database and ghost containers to prevent database locks:
```bash
rm -rf ./logs/*
docker compose down --volumes --remove-orphans

### 2. Boot the Environment
Run the automated startup script. This will build the lightweight images, establish the secure networks, and configure the database.

```bash
./start_local.sh
```

### 3. Access the Dashboard
Once the script says Admin user successfully verified!, wait about 10-20 seconds for the webserver workers to warm up.

URL: http://localhost:4040
Username: admin
Password: admin

## Important Notes on Testing
Because this is a highly optimized, air-gapped sandbox, you will notice specific behaviors when interacting with the DAGs:

DAG Parsing Works: You will see all the project's DAGs load successfully in the UI. The graph views, code views, and task dependencies will render perfectly.

DAG Execution Will Fail: If you manually trigger a DAG, the tasks will eventually fail. This is expected and by design.

The network airgap prevents the containers from downloading external data.

The mocked Python libraries (xarray, pandas, etc.) act as structural placeholders for parsing, but do not actually process data during execution.

## Teardown
To stop the environment and completely wipe the state (including the local database):

'''Bash
docker compose down --volumes --remove-orphans
rm -rf ./logs/*
```