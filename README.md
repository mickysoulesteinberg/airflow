# Airflow Project

## Overview
Getting setup with Airflow.

## Prerequisites
- **Mac** with Docker Desktop installed  
- **Docker Compose** (comes with Docker Desktop)  
- **Python** (optional, only if running commands locally)  
- **Environment variables**: copy `.env.example` → `.env` and set:  
  - `AIRFLOW_PROJ_DIR=...`  
  - `GOOGLE_APPLICATION_CREDENTIALS=...`  
  - `GMAIL_USER=...`  
  - `GMAIL_PASS=...`  
  - (add any others)

## Setup
1. Clone this repo:
    ```bash
    git clone <repo_url>
    cd <repo_name>
    ```

2. Copy and edit env file:
    ``` bash
    cp .env.example .env
    ```
    Fill in required values.

3. Start containers:
    ``` bash
    docker compose up -d
    ```

4. Initialize Airflow (only first time):
    ``` bash
    docker compose up airflow-init
    ```

5. Access Airflow UI:
    - Go to http://localhost:8080
    - Default login: `airflow`/`airflow`

## Usage
- Start services:
    ``` bash
    docker compose up -d
    ```

- Stop services:
    ``` bash
    docker compose down
    ```

- (Optional) Create an alias to start Airflow using airflow-webserver:
    Add the following line to the shell configuration file (for me, `~/.zshrc`):
    ``` bash
    alias start-airflow="docker compose exec -it airflow-webserver bash"
    ```

## DAGs
- test_email_to_gcs: First DAG to test sending a csv sent via email to a table in BigQuery

## Scratchpad
- Commands I ran:

- Errors I hit and fixes:

# Spotify 

## Setup

1. Create an app (Web API) at developer.spotify.com. See [Getting Started with Web API](https://developer.spotify.com/documentation/web-api/tutorials/getting-started)



