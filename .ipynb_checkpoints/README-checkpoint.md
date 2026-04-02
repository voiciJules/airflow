# Build ETL Data Pipelines with BashOperator and PythonOperator using Apache Airflow

## Project Overview
This project demonstrates the design and implementation of an ETL data pipeline using Apache Airflow, leveraging both BashOperator and PythonOperator.  
  
The pipeline processes toll data by extracting, transforming, and consolidating data from multiple sources into a unified  dataset.  
  
## What I Built
- Designed an end-to-end ETL pipeline using Apache Airflow  DAGs   
- Automated workflow orchestration with task dependencies  
- Implemented data extraction from multiple file formats:  
  - CSV  
  - TSV  
  - Fixed-width text files  
- Consolidated and  transformed datasets into a single structured output  
  
## Key Technologies
- Apache Airflow  
- Python  
- Bash (shell scripting)  
- CSV / TSV / Fixed-width file processing
  
## What I Learned
1. Workflow Orchestration with Airflow
   - Defined DAGs and task dependencies
   - Managed execution order using task pipelines
   - Monitored DAG runs via CLI and Web UI
   
2. BashOperator vs PythonOperator
   - BashOperator
     - Used shell commands (cut, paste, tar) for efficient data extraction and transformation
     - Learned how to quickly manipulate structured and semi-structured files using Linux tools
  
   - PythonOperator
     - Built reusable Python functions for:
       - Data ingestion (API download)
       - File extraction (tarfile)
       - Data processing (CSV manipulation)
       - Applied modular and maintainable pipeline design
  
3. Data Processing Techniques
   - Extracted specific fields from different file formats
   - Handled delimiter-based (CSV/TSV) and positional (fixed-width) data
   - Performed transformations such as string normalization (uppercase conversion)
  
4. Pipeline Design Best Practices
   - Separated staging and processed data layers
   - Structured pipelines following ETL/ELT principles

5. Airflow CLI Commands Used
   - airflow dag list
   - airflow dag list | grep ETL_toll_data
   - airflow dags trigger ETL_toll_data
   - airflow dags list-run -d ETL_toll_data
   - airflow dags list-import-errors
   - airflow tasks list ETL_toll_data
   - airflow webserver

## Key Takeaway
This project helped me understand how to build and orchestrate scalable data pipelines by combining system-level scripting (Bash) with flexible data processing logic (Python), reflecting real-world data engineering workflows.
