FROM --platform=linux/amd64 ghcr.io/astral-sh/uv:debian-slim

ENV UV_LINK_MODE=copy
# Docker run launcher example
RUN mkdir -p /opt/dagster/dagster_home /opt/dagster/app /opt/dagster/code

RUN useradd -s /bin/bash -d /opt/dagster/dagster_home/ dagster && \
    chown -R dagster: /opt/dagster

RUN apt update && \
    apt install -y git wget gnupg2 && \
    apt clean && \
    apt autoremove -y && \
    rm -rf /var/lib/apt/lists/*

# Install packages needed to talk to edxapp mongodb
#RUN apt-get update && \
#    apt-get install -y wget gnupg software-properties-common && \
#    wget -qO - https://www.mongodb.org/static/pgp/server-7.0.asc | gpg --dearmor > /etc/apt/trusted.gpg.d/mongodb-7.0.gpg && \
#    echo "deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/ubuntu jammy/mongodb-org/7.0 multiverse" | tee /etc/apt/sources.list.d/mongodb-org-7.0.list && \
#    apt-get update && \
#    apt-get install -y mongodb-database-tools && \
#    apt-get clean && \
#    apt-get autoremove -y && \
#    rm -rf /var/lib/apt/lists/*


ENV DAGSTER_HOME=/opt/dagster/dagster_home/

USER dagster
EXPOSE 3000
WORKDIR /opt/dagster/code
# Copy poetry project to the $WORKDIR
COPY pyproject.toml uv.lock /opt/dagster/code

RUN uv sync --locked --no-dev --no-install-project
