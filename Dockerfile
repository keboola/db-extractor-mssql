FROM quay.io/keboola/docker-base-php56:0.0.2
MAINTAINER Erik Zigo <erik.zigo@keboola.com>

RUN yum -y --enablerepo=epel,remi,remi-php56 install php-mssql

# MSSQL
ADD mssql/freetds.conf /etc/freetds.conf

WORKDIR /home

# Initialize
COPY . /home/
RUN composer install --no-interaction

CMD php ./src/run.php --data=/data