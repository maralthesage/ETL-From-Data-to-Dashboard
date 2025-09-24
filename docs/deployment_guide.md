# Deployment Guide

This guide provides step-by-step instructions for deploying the ETL pipeline in different environments.

## 🚀 Quick Start

### Prerequisites

- Python 3.8 or higher
- pip or conda package manager
- Git (for version control)
- Docker (optional, for containerized deployment)

### Local Development Setup

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd ETL-From-Data-to-Dashboard
   ```

2. **Create virtual environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Set up environment variables**
   ```bash
   export DATA_PATH="/path/to/your/data"
   export OUTPUT_PATH="/path/to/your/output"
   export ENVIRONMENT="development"
   ```

5. **Run the pipeline**
   ```bash
   python src/main.py --mode complete
   ```

## 🐳 Docker Deployment

### Create Dockerfile

```dockerfile
FROM python:3.9-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY src/ ./src/
COPY config/ ./config/

# Set environment variables
ENV PYTHONPATH=/app
ENV DATA_PATH=/app/data
ENV OUTPUT_PATH=/app/output

# Create data directories
RUN mkdir -p /app/data /app/output

# Run the application
CMD ["python", "src/main.py", "--mode", "complete"]
```

### Build and Run Docker Container

```bash
# Build the image
docker build -t etl-pipeline .

# Run the container
docker run -v /path/to/data:/app/data -v /path/to/output:/app/output etl-pipeline
```

## ☁️ Cloud Deployment

### AWS Deployment

#### Using AWS Batch

1. **Create ECR repository**
   ```bash
   aws ecr create-repository --repository-name etl-pipeline
   ```

2. **Build and push Docker image**
   ```bash
   aws ecr get-login-password --region us-west-2 | docker login --username AWS --password-stdin <account-id>.dkr.ecr.us-west-2.amazonaws.com
   docker build -t etl-pipeline .
   docker tag etl-pipeline:latest <account-id>.dkr.ecr.us-west-2.amazonaws.com/etl-pipeline:latest
   docker push <account-id>.dkr.ecr.us-west-2.amazonaws.com/etl-pipeline:latest
   ```

3. **Create job definition**
   ```json
   {
     "jobDefinitionName": "etl-pipeline",
     "type": "container",
     "containerProperties": {
       "image": "<account-id>.dkr.ecr.us-west-2.amazonaws.com/etl-pipeline:latest",
       "vcpus": 2,
       "memory": 4096,
       "environment": [
         {
           "name": "DATA_PATH",
           "value": "s3://your-bucket/data/"
         },
         {
           "name": "OUTPUT_PATH", 
           "value": "s3://your-bucket/output/"
         }
       ]
     }
   }
   ```

#### Using AWS Lambda

1. **Create Lambda deployment package**
   ```bash
   pip install -r requirements.txt -t lambda-package/
   cp -r src/ lambda-package/
   cp -r config/ lambda-package/
   cd lambda-package && zip -r ../etl-pipeline.zip .
   ```

2. **Deploy Lambda function**
   ```bash
   aws lambda create-function \
     --function-name etl-pipeline \
     --runtime python3.9 \
     --role arn:aws:iam::<account-id>:role/lambda-execution-role \
     --handler src.main.lambda_handler \
     --zip-file fileb://etl-pipeline.zip
   ```

### Azure Deployment

#### Using Azure Container Instances

1. **Build and push to Azure Container Registry**
   ```bash
   az acr build --registry <registry-name> --image etl-pipeline .
   ```

2. **Deploy container instance**
   ```bash
   az container create \
     --resource-group <resource-group> \
     --name etl-pipeline \
     --image <registry-name>.azurecr.io/etl-pipeline:latest \
     --cpu 2 \
     --memory 4 \
     --environment-variables \
       DATA_PATH=abfss://<container>@<storage>.dfs.core.windows.net/data/ \
       OUTPUT_PATH=abfss://<container>@<storage>.dfs.core.windows.net/output/
   ```

#### Using Azure Functions

1. **Create function app**
   ```bash
   az functionapp create \
     --resource-group <resource-group> \
     --consumption-plan-location <location> \
     --runtime python \
     --runtime-version 3.9 \
     --functions-version 4 \
     --name <function-app-name> \
     --storage-account <storage-account>
   ```

2. **Deploy function code**
   ```bash
   func azure functionapp publish <function-app-name>
   ```

### Google Cloud Deployment

#### Using Cloud Run

1. **Build and push to Google Container Registry**
   ```bash
   gcloud builds submit --tag gcr.io/<project-id>/etl-pipeline
   ```

2. **Deploy to Cloud Run**
   ```bash
   gcloud run deploy etl-pipeline \
     --image gcr.io/<project-id>/etl-pipeline \
     --platform managed \
     --region us-central1 \
     --memory 4Gi \
     --cpu 2 \
     --set-env-vars DATA_PATH=gs://<bucket>/data/,OUTPUT_PATH=gs://<bucket>/output/
   ```

#### Using Cloud Functions

1. **Deploy function**
   ```bash
   gcloud functions deploy etl-pipeline \
     --runtime python39 \
     --trigger-http \
     --source . \
     --entry-point main \
     --memory 4GB \
     --timeout 540s
   ```

## 🔧 Prefect Deployment

### Local Prefect Server

1. **Start Prefect server**
   ```bash
   prefect server start
   ```

2. **Create deployment**
   ```bash
   prefect deployment build src/main.py:run_complete_etl_pipeline -n "customer-etl"
   prefect deployment apply run_complete_etl_pipeline-deployment.yaml
   ```

3. **Run deployment**
   ```bash
   prefect deployment run "customer-etl/run_complete_etl_pipeline"
   ```

### Prefect Cloud

1. **Authenticate with Prefect Cloud**
   ```bash
   prefect cloud login
   ```

2. **Create workspace**
   ```bash
   prefect cloud workspace create --name "etl-pipeline-workspace"
   ```

3. **Deploy flows**
   ```bash
   prefect deployment build src/main.py:run_complete_etl_pipeline -n "customer-etl-cloud"
   prefect deployment apply run_complete_etl_pipeline-deployment.yaml
   ```

## 📊 Monitoring and Logging

### Application Monitoring

1. **Enable Prefect monitoring**
   ```python
   from prefect import get_run_logger
   
   logger = get_run_logger()
   logger.info("Pipeline started")
   ```

2. **Add custom metrics**
   ```python
   import time
   
   start_time = time.time()
   # ... processing ...
   processing_time = time.time() - start_time
   logger.info(f"Processing completed in {processing_time:.2f} seconds")
   ```

### Log Aggregation

#### Using ELK Stack

1. **Configure Logstash**
   ```yaml
   input {
     file {
       path => "/var/log/etl-pipeline/*.log"
       type => "etl-pipeline"
     }
   }
   
   filter {
     if [type] == "etl-pipeline" {
       grok {
         match => { "message" => "%{TIMESTAMP_ISO8601:timestamp} - %{WORD:logger} - %{WORD:level} - %{GREEDYDATA:message}" }
       }
     }
   }
   
   output {
     elasticsearch {
       hosts => ["elasticsearch:9200"]
       index => "etl-pipeline-logs"
     }
   }
   ```

2. **Create Kibana dashboards**
   - Processing time trends
   - Error rate monitoring
   - Data quality metrics

#### Using CloudWatch (AWS)

1. **Configure CloudWatch agent**
   ```json
   {
     "logs": {
       "logs_collected": {
         "files": {
           "collect_list": [
             {
               "file_path": "/var/log/etl-pipeline/application.log",
               "log_group_name": "/aws/etl-pipeline",
               "log_stream_name": "{instance_id}"
             }
           ]
         }
       }
     }
   }
   ```

2. **Create CloudWatch alarms**
   - Processing time > threshold
   - Error rate > threshold
   - Data quality issues

## 🔒 Security Considerations

### Data Encryption

1. **Encrypt data at rest**
   ```bash
   # AWS S3
   aws s3api put-bucket-encryption \
     --bucket your-bucket \
     --server-side-encryption-configuration '{
       "Rules": [{
         "ApplyServerSideEncryptionByDefault": {
           "SSEAlgorithm": "AES256"
         }
       }]
     }'
   ```

2. **Encrypt data in transit**
   ```python
   # Use HTTPS for all API calls
   import ssl
   ssl_context = ssl.create_default_context()
   ```

### Access Control

1. **IAM roles and policies**
   ```json
   {
     "Version": "2012-10-17",
     "Statement": [
       {
         "Effect": "Allow",
         "Action": [
           "s3:GetObject",
           "s3:PutObject"
         ],
         "Resource": [
           "arn:aws:s3:::your-bucket/data/*",
           "arn:aws:s3:::your-bucket/output/*"
         ]
       }
     ]
   }
   ```

2. **Network security**
   ```yaml
   # VPC configuration
   vpc:
     cidr: "10.0.0.0/16"
     subnets:
       - cidr: "10.0.1.0/24"
         availability_zone: "us-west-2a"
       - cidr: "10.0.2.0/24"
         availability_zone: "us-west-2b"
   ```

## 🚨 Troubleshooting

### Common Issues

1. **Memory issues**
   ```bash
   # Increase memory limit
   export DASK_MEMORY_LIMIT="8GB"
   ```

2. **File permission issues**
   ```bash
   # Fix permissions
   chmod -R 755 /path/to/data
   chmod -R 755 /path/to/output
   ```

3. **Network connectivity issues**
   ```bash
   # Test connectivity
   ping your-data-source
   telnet your-data-source 443
   ```

### Debug Mode

1. **Enable debug logging**
   ```bash
   export LOG_LEVEL="DEBUG"
   python src/main.py --mode complete
   ```

2. **Run with verbose output**
   ```bash
   python -v src/main.py --mode complete
   ```

### Performance Optimization

1. **Use Dask for large datasets**
   ```python
   from dask.distributed import Client
   
   client = Client(n_workers=4, memory_limit='8GB')
   ```

2. **Optimize data processing**
   ```python
   # Use chunked processing
   chunk_size = 10000
   for chunk in pd.read_csv(file, chunksize=chunk_size):
       process_chunk(chunk)
   ```

## 📈 Scaling

### Horizontal Scaling

1. **Multiple workers**
   ```bash
   # Run multiple instances
   python src/main.py --mode complete &
   python src/main.py --mode complete &
   ```

2. **Load balancing**
   ```yaml
   # Docker Compose
   version: '3'
   services:
     etl-worker-1:
       image: etl-pipeline
       environment:
         - WORKER_ID=1
     etl-worker-2:
       image: etl-pipeline
       environment:
         - WORKER_ID=2
   ```

### Vertical Scaling

1. **Increase resources**
   ```bash
   # AWS Batch
   aws batch update-job-queue \
     --job-queue etl-pipeline-queue \
     --compute-environment-order 1,environment=etl-pipeline-env
   ```

2. **Optimize memory usage**
   ```python
   # Use memory-efficient data types
   df['customer_id'] = df['customer_id'].astype('category')
   df['date'] = pd.to_datetime(df['date'])
   ```

## 🔄 CI/CD Pipeline

### GitHub Actions

```yaml
name: ETL Pipeline CI/CD

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Set up Python
        uses: actions/setup-python@v2
        with:
          python-version: 3.9
      - name: Install dependencies
        run: |
          pip install -r requirements.txt
      - name: Run tests
        run: pytest tests/ -v

  deploy:
    needs: test
    runs-on: ubuntu-latest
    if: github.ref == 'refs/heads/main'
    steps:
      - uses: actions/checkout@v2
      - name: Deploy to production
        run: |
          # Your deployment commands here
```

### GitLab CI

```yaml
stages:
  - test
  - deploy

test:
  stage: test
  script:
    - pip install -r requirements.txt
    - pytest tests/ -v

deploy:
  stage: deploy
  script:
    - docker build -t etl-pipeline .
    - docker push registry.gitlab.com/your-group/etl-pipeline
  only:
    - main
```

This deployment guide provides comprehensive instructions for deploying the ETL pipeline in various environments and scenarios.
