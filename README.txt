# Reference Data Project

## Overview
This is a mini-project to learn about **reference data operations** in financial markets.  
The project builds a pipeline that ingests raw market data, applies validation and normalization rules, and produces a clean "golden source" dataset that can be reused for analysis.

While this project started as a learning exercise, the pipeline is designed to be flexible and **reusable**. Moving forward, it can be extended for **personal investing and market analysis** workflows, such as:
- Tracking equity and ETF performance
- Monitoring dual listings and corporate actions
- Running custom analytics or dashboards for investment decisions

## Key Features
- Ingests raw instrument/price data
- Cleans and normalizes identifiers across listings
- Applies validation checks to ensure data quality
- Produces a consistent **golden copy** dataset
- Modular design for reusability

## Next Steps
- Add dashboards for personal market analysis
- Enhance validation rules and logging
- Integrate with personal investing workflows

## Project Structure
project-root/
│── conf/ # Configurations (database, connections, parameters)
│── etl/ # ETL jobs and transformations
│── sql/ # SQL scripts for schema and validation
│── run_ingest.py # Run data ingestion
│── run_build_core.py # Build golden source
│── requirements.txt # Python dependencies
│── .gitignore # Ignored files/folders