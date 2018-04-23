FROM php:7.1.3-fpm
ENV DEBIAN_FRONTEND=noninteractive
ENV COMPOSER_ALLOW_SUPERUSER=1

RUN apt-get update; \
    apt-get install -y --no-install-recommends curl unzip gzip tar git apt-transport-https wget ssh libxml2-dev

RUN apt-get remove -y binutils

RUN echo "\r\n deb http://ftp.de.debian.org/debian/ sid main" >> /etc/apt/sources.list && \
    apt-get update && \
    apt-get -y install tdsodbc \
    php7.1-sybase \
    gcc \
    build-essential

WORKDIR /code

RUN curl -sS ftp://ftp.freetds.org/pub/freetds/stable/freetds-patched.tar.gz > freetds-patched.tar.gz
RUN tar xzvf freetds-patched.tar.gz
RUN mkdir /tmp/freetds && mv freetds-*/* /tmp/freetds/

RUN cd /tmp/freetds && \
    ./configure --enable-msdblib --prefix=/usr/local && \
    make -j$(nproc) && \
    make install && \
      docker-php-ext-install pdo_dblib && \
      sed -i '$ d' /etc/apt/sources.list

# MSSQL
ADD mssql/freetds.conf /etc/freetds.conf

RUN echo "memory_limit = -1" >> /usr/local/etc/php/php.ini

RUN curl -sS https://getcomposer.org/installer | php \
  && mv composer.phar /usr/local/bin/composer

# Initialize
ADD . /code
WORKDIR /code

RUN composer install --no-interaction

CMD php ./src/run.php --data=/data