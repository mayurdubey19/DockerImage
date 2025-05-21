FROM apache/airflow:2.10.4-python3.8

USER root
ENV AIRFLOW_HOME=/opt/airflow

# Install system-level packages
RUN apt-get update && \
    apt-get install -y git && \
    apt-get clean

# Copy requirements.txt first to take advantage of Docker layer caching
COPY requirements.txt /requirements.txt

# Install custom Python packages from requirements.txt and nvd3 (as airflow user)
USER airflow
RUN pip install --no-cache-dir -r /requirements.txt && \
    pip install git+https://github.com/areski/python-nvd3.git

# Switch back to root to copy entrypoint and change permissions
USER root
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Switch back to airflow for runtime
USER airflow
#COPY . /opt/airflow/dags

EXPOSE 8080 5432
ENTRYPOINT ["/entrypoint.sh"]