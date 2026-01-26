ARG MSSQL_VERSION=2022

FROM mcr.microsoft.com/mssql/server:${MSSQL_VERSION}-latest

USER root

RUN apt-get update && apt-get install -y \
    netcat \
    curl \
    gnupg2 \
    && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
    && curl -fsSL https://packages.microsoft.com/config/ubuntu/20.04/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y mssql-tools \
    && rm -rf /var/lib/apt/lists/*

ENV PATH="${PATH}:/opt/mssql-tools/bin"

RUN /opt/mssql/bin/mssql-conf set sqlagent.enabled true
