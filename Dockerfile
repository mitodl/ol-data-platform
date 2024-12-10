FROM --platform=linux/amd64 python:3.13-slim AS dagster-base

# Docker run launcher example
RUN mkdir -p /opt/dagster/dagster_home /opt/dagster/app

# Install necessary dependencies
RUN pip install dagster dagster-docker dagster-webserver dagster-postgres dagster-webserver

RUN mkdir -p /opt/dagster/dagster_home /opt/dagster/app && \
    useradd -s /bin/bash -d /opt/dagster/dagster_home/ dagster && \
    chown -R dagster: /opt/dagster

RUN apt update && \
    apt install -y git wget gnupg2 && \
    apt clean && \
    apt autoremove -y && \
    rm -rf /var/lib/apt/lists/*


# Install packages needed to talk to edxapp mongodb
RUN wget -qO - https://www.mongodb.org/static/pgp/server-5.0.asc | apt-key add - && \
    echo "deb http://repo.mongodb.org/apt/debian buster/mongodb-org/5.0 main" > /etc/apt/sources.list.d/mongodb-org-5.0.list && \
    apt update && \
    apt install -y mongodb-org-tools && \
    apt clean && \
    apt autoremove && \
    rm -rf /var/lib/apt/lists/*

RUN pip install poetry

ENV DAGSTER_HOME=/opt/dagster/dagster_home/

USER dagster
EXPOSE 3000
WORKDIR /opt/dagster/code
# Copy poetry project to the $WORKDIR
COPY pyproject.toml /opt/dagster/code

RUN poetry install --without=dev --no-cache
