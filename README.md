# Project Setup

## Prerequisites

Make sure the following are installed on your system:

* Python
* UV
* Docker

## Run the Project

1. Clone the repository:

```bash
git clone <repository-url>
cd <project-directory>
```

2. Install dependencies:

```bash
uv sync
```

3. Start the required services:

```bash
docker compose up -d
```

## Access Airflow

After the Docker containers are created successfully, open the Airflow UI in your browser:

```
http://localhost:8080
```

### Login Credentials

* **Username:** airflow
* **Password:** airflow
