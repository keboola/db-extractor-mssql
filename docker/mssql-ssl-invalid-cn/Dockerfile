ARG MSSQL_VERSION=2022

FROM mcr.microsoft.com/mssql/server:${MSSQL_VERSION}-latest

USER root

COPY tests/ssl-certs/mssql-invalidCn.crt /etc/ssl/certs/mssql.crt
COPY tests/ssl-certs/mssql-invalidCn.key /etc/ssl/private/mssql.key

RUN chmod 600 /etc/ssl/certs/mssql.crt /etc/ssl/private/mssql.key

RUN /opt/mssql/bin/mssql-conf set network.tlscert /etc/ssl/certs/mssql.crt \
      && /opt/mssql/bin/mssql-conf set network.tlskey /etc/ssl/private/mssql.key \
      && /opt/mssql/bin/mssql-conf set network.tlsprotocols 1.2 \
      && /opt/mssql/bin/mssql-conf set network.forceencryption 1
