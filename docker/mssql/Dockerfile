ARG MSSQL_VERSION=2022

FROM mcr.microsoft.com/mssql/server:${MSSQL_VERSION}-latest

USER root

RUN /opt/mssql/bin/mssql-conf set sqlagent.enabled true
