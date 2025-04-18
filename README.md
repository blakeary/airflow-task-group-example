# Apache Airflow Docker Compose Setup

This repository contains a Docker Compose configuration for running Apache Airflow in a development environment on Windows.

## Components

- PostgreSQL (metadata database)
- Redis (message broker)
- Airflow Webserver
- Airflow Scheduler
- Airflow Worker
- Airflow Triggerer
- Flower (Celery monitoring tool)

## Quick Start

1. Create a `.env` file with: `AIRFLOW_UID=50000`
2. Run: `docker-compose up -d`

## Access Details

### Airflow Webserver (UI)
- URL: http://localhost:8080
- Username: **airflow**
- Password: **airflow**

### Flower (Celery Monitoring)
- URL: http://localhost:5555

## Tips

- Place DAG files in the `dags/` directory
- First run may take several minutes to initialize
- Shutdown with: `docker-compose down`