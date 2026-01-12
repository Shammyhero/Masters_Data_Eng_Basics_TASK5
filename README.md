# Restaurant ETL Pipeline

This project runs a data pipeline using Apache Airflow to process and clean restaurant data. It is containerized with Docker for easy setup.

## Project Overview

The pipeline performs the following steps in order:

1.  **Extract:** Reads multiple CSV files from the input directory and merges them into a single dataset.
2.  **Enrich:** Identifies records with missing latitude and longitude values and fetches them using the OpenCage Geocoding API.
3.  **Transform:** Generates a Geohash for each location to help with geospatial indexing.
4.  **Load:** Saves the final processed data to the `destination` folder in both Parquet and CSV formats.

## Setup and Usage

**Prerequisites:**
*   Docker and Docker Compose installed on your machine.
*   An API key from OpenCage Data.

**Instructions:**

1.  Open the `.env` file in the project root.
2.  Add your OpenCage API key to the variable `AIRFLOW_VAR_OPENCAGE_API_KEY`.
3.  Start the services:
    ```bash
    docker compose up -d
    ```
4.  Access the Airflow UI by visiting `http://localhost:8080` in your browser.
5.  Log in with the username `airflow` and password `airflow`.
6.  Locate the DAG named `restaurant_etl_modular` and trigger it to run the pipeline.

## Dependencies

*   **Apache Airflow:** Orchestrates the workflow.
*   **Docker:** Handles the environment and services (Postgres, Redis, Airflow).
*   **Pandas:** Used for data manipulation and merging.
*   **PyGeohash:** Used to generate geohashes from coordinates.

## Workflow overview:

<img width="1062" height="649" alt="Screenshot 2026-01-12 at 21 24 43" src="https://github.com/user-attachments/assets/a79a0c40-37c3-4248-91f6-cff67ce2a043" />

