# Overview

This repository holds the business logic for building and managing the data pipelines used to power various data
services at MIT Open Learning. The core framework is Dagster which provides a flexible, and well structured approach to
building data applications.

# Running Dagster Locally via Docker
- Ensure that you have the latest version of Docker installed.
    https://www.docker.com/products/docker-desktop/
- Install docker compose. Check the documentation and requirements for your specific machine.
    https://docs.docker.com/compose/install/
- Ensure you are able to authenticate into GitHub + Vault
    https://github.com/mitodl/ol-data-platform/tree/main
    https://vault-qa.odl.mit.edu/v1/auth/github/login
    `vault login -address=https://vault-qa.odl.mit.edu -method=github`
    https://vault-production.odl.mit.edu/v1/auth/github/login
    `vault login -address=https://vault-production.odl.mit.edu -method=github`
- Ensure you create your .env file and populate it with the environment variables.
    `cp .env.example .env`
- Call docker compose up
    `docker compose up --build`
- Navigate to localhost:3000 to access the Dagster UI
