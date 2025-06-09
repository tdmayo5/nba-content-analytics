FROM apache/airflow:2.7.1-python3.9

# Switch to root to install system dependencies
USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

# Copy requirements first for better Docker layer caching
COPY requirements.txt /tmp/requirements.txt

# Install Python packages
RUN pip install --no-cache-dir --user -r /tmp/requirements.txt

# Install dbt separately to avoid conflicts
RUN pip install --no-cache-dir --user dbt-postgres==1.6.0

# Set environment variables
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow:/opt/airflow/data_generators"
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
ENV AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=False

# Create necessary directories
USER root
RUN mkdir -p /opt/airflow/dags /opt/airflow/plugins /opt/airflow/data /opt/airflow/dbt /opt/airflow/data_generators
RUN chown -R airflow:airflow /opt/airflow
USER airflow

# Copy project files (this should be done by volume mounts in development)
# COPY . /opt/airflow/

WORKDIR /opt/airflow