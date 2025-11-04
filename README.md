# MovieLens LakeHouse Pipeline

This project implements a MovieLens LakeHouse pipeline demonstrating modern data engineering practices.

## Setup

1.  **Prerequisites**:
    *   Docker
    *   Docker Compose
    *   Python 3.9+

2.  **Clone the repository**:

    ```bash
    git clone <repository_url>
    cd Urban Mobility Optimization and Environmental Impact Monitoring
    ```

3.  **Environment Variables**:

    Copy the example environment file and modify as needed:

    ```bash
    cp .env.example .env
    ```

4.  **Install Python Dependencies**:

    ```bash
    make setup
    ```

## Usage

### Start Services

To start all services (Kafka, Zookeeper, Minio, Spark, Airflow):

```bash
make up
```

### Stop Services

```bash
make down
```

### View Logs

```bash
make logs
```

## Project Structure

```
project-root/
├── src/                 # Source code for ingestion, streaming, processing, config, utils
├── tests/               # Unit and integration tests
├── airflow/             # Airflow DAG definitions and plugins
├── docker/              # Dockerfiles and configurations for services
├── notebooks/           # Jupyter notebooks for ML experiments and EDA
├── Data/                # Raw data files (MovieLens dataset)
├── docs/                # Project documentation
├── docker-compose.yml   # Docker Compose orchestration file
├── requirements.txt     # Python dependencies
├── .env.example         # Template for environment variables
├── .gitignore           # Git ignore rules
├── Makefile             # Common commands
└── README.md            # Project documentation
```

## Development

### Running Tests

```bash
make test
```

### Linting and Formatting

```bash
make lint
make format
```

## Documentation

Refer to the `docs/` directory for detailed architecture, setup, and development guides.