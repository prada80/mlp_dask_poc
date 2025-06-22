FROM apache/airflow:2.10.5-python3.10

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    build-essential \
    libffi-dev \
    libssl-dev \
    python3-dev \
    libpq-dev \
    git \
 && apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow

# Install required Python libraries
RUN pip install --no-cache-dir \
    drain3 \
    boto3 \
    pandas \
    "dask[complete]==2023.12.1" \
    "distributed==2023.12.1" \
    s3fs \
    "great_expectations>=1.4.2" \
    scikit-learn \
    matplotlib \
    seaborn \
    mlflow \
    tensorflow==2.13.0 \
    keras==2.13.1 \
    transformers==4.31.0 \
    huggingface_hub
