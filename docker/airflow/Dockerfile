FROM apache/airflow:2.7.1-python3.9

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    git \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Copy requirements and install
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Install dbt
RUN pip install dbt-postgres==1.6.0

# Copy project files
COPY . /opt/airflow/

# Set Python path
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow"
