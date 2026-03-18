# Traffic Accident ETL & Visualization Project

# Purpose
This repository demonstrates an end-to-end ETL (Extract, Transform, Load) pipeline for Taiwan traffic accident data, featuring a frontend dashboard built with Streamlit. The ultimate goal is to deploy the service as a microservice on Google Cloud Platform (GCP) using Cloud Run.

# Developement workflow flow in each branch
The project is organized into four sequential branches. Each branch represents a specific milestone in the development lifecycle, including its core functions, environment, and deliverables.

# Branch 1, name: "feature/etl-app"
1. core func.: Establish the core ETL logic and ensure data insights are correctly visualized via Streamlit. This branch uses Poetry for dependency management and virtual environment control. All scripts must pass unit tests before being merged into subsequent branches.
2. environment: python + MySQL on premises
3. planned directories:
    ```
        my_project/
            ├── src/
            │   ├── transform.py       # ETL 核心邏輯
            │   └── app.py             # Streamlit 程式碼
            ├── tests/
            │   └── test_transform.py  # Unittest 測試檔
            ├── pyproject.toml         # Poetry 設定
            ├── poetry.lock            # 精確版本鎖定
            └── .env                   # 本地環境變數(e.g, DB_HOST=localhost)
    ```
# Branch 2, name: "feature/docker-integration"
1. core func.: Containerize MySQL, Streamlit, and Apache Airflow. Airflow is utilized to schedule and automate ETL processes. This stage focuses on ensuring seamless communication and networking between containers.
2. sources: merged from Branch 1 and pyproject.toml. ``Any modifications to database connections or service networking are handled in this branch.``
3. planned directories:
    ```
        my-project/
            ├── dags/                  # + 存放Airflow DAGs
            ├── docker/                # + 容器定義
            │   ├── Dockerfile.airflow
            │   └── Dockerfile.streamlit
            ├── src/                   # (From Branch 1)
            ├── tests/                 # (From Branch 1)
            ├── docker-compose.yml     # + 一鍵啟動所有容器
            ├── pyproject.toml         # (From Branch 1)
            └── requirements.txt       # + 執行poetry export產出
    ```
# Branch 3, name: "develop/CI"
1. core func.: Implement GitHub Actions to automate the build and push processes. This ensures that Docker images are automatically validated and stored in a container registry upon code updates.
2. sources: all components from Branch 2.
3. planned directories:
    ```
        my-project/
            ├── .github/           # + GitHub Actions自動化腳本
            │   └── workflows/
            │       └── ci-cd.yml  # 測試build image並push到Artifact Registry
            ├── dags/
            ├── docker/
            ├── src/
            ├── docker-compose.yml
            ├── requirements.txt
            └── .env.example       # + 提供給雲端環境的變數範本
    ```
# Branch 4, name: "main/production"
1. core func: Production-ready branch for stable service deployment on GCP.
2. sources: Merged from develop/CI after all CI/CD checks pass.
3. directories:
    ```
        my-project/
            ├── (those from branch 3)
            └── README.md  # 也就是本文。且未來會再附上Cloud Run網址與VM操作說明
    ```
