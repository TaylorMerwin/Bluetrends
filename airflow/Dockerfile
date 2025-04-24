FROM apache/airflow:2.10.5

# Install providers and runtime dependencies as the airflow user
USER airflow
RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark \
    apache-airflow-providers-mysql \
    pandas \
    requests

# Switch back to root to ensure proper permissions on default entrypoint
USER root

# install Docker Engine CLI + Compose plugin
RUN apt-get update \
 && apt-get install -y docker.io docker-compose-plugin \
 && rm -rf /var/lib/apt/lists/*

# add the airflow user to the docker group so `docker` commands don’t need root
RUN usermod -aG docker airflow

# No custom entrypoint; we'll initialize the DB and create users manually
# The default ENTRYPOINT and CMD from the base image remain unchanged

# Switch back to airflow user for runtime
USER airflow
