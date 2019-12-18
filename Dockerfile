FROM keboola/db-component-ssh-proxy:latest AS sshproxy
FROM php:7.4-cli

ARG DEBIAN_FRONTEND=noninteractive
ARG COMPOSER_FLAGS="--prefer-dist --no-interaction"
ENV COMPOSER_ALLOW_SUPERUSER=1
ENV COMPOSER_PROCESS_TIMEOUT 3600

RUN apt-get update -q \
  && apt-get install -y --no-install-recommends \
  unzip git apt-transport-https wget ssh libxml2-dev gnupg2 unixodbc-dev libgss3

RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
  && curl https://packages.microsoft.com/config/debian/9/prod.list > /etc/apt/sources.list.d/mssql-release.list \
  && apt-get update \
  && ACCEPT_EULA=Y apt-get install -y msodbcsql17 mssql-tools

RUN pecl install pdo_sqlsrv-5.7.1preview sqlsrv-5.7.1preview \
  && docker-php-ext-enable sqlsrv pdo_sqlsrv \
  && docker-php-ext-install xml

RUN echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bashrc \
  && /bin/bash -c "source ~/.bashrc"

ENV PATH $PATH:/opt/mssql-tools/bin

RUN echo "memory_limit = -1" >> /usr/local/etc/php/php.ini

WORKDIR /root

RUN curl -sS https://getcomposer.org/installer | php \
  && mv composer.phar /usr/local/bin/composer

WORKDIR /code

## Composer - deps always cached unless changed
# First copy only composer files
COPY composer.* /code/
# Download dependencies, but don't run scripts or init autoloaders as the app is missing
RUN composer install $COMPOSER_FLAGS --no-scripts --no-autoloader
# copy rest of the app
COPY . /code/
# run normal composer - all deps are cached already
RUN composer install $COMPOSER_FLAGS

COPY --from=sshproxy /root/.ssh /root/.ssh

CMD php ./src/run.php --data=/data
