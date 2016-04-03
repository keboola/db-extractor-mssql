FROM quay.io/keboola/docker-base-php56:0.0.2
MAINTAINER Erik Zigo <erik.zigo@keboola.com>

RUN yum -y --enablerepo=epel,remi,remi-php56 install php-mssql
#RUN yum -y --enablerepo=epel,remi,remi-php56 install php-odbc

#ADD driver/odbc.ini /etc/odbc.ini
#ENV ODBCINI /etc/odbc.ini


WORKDIR /home

# Initialize
COPY . /home/
RUN composer install --no-interaction

RUN curl --location --silent --show-error --fail \
        https://github.com/Barzahlen/waitforservices/releases/download/v0.3/waitforservices \
        > /usr/local/bin/waitforservices && \
    chmod +x /usr/local/bin/waitforservices

ENTRYPOINT php ./src/run.php --data=/data