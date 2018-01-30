FROM lbosqmsft/mssql-php-msphpsql

ENV COMPOSER_ALLOW_SUPERUSER=1

RUN apt-get update -q && apt-get install ssh git zip wget unzip time libzip-dev -y --no-install-recommends

WORKDIR /root

RUN curl -sS https://getcomposer.org/installer | php \
  && mv composer.phar /usr/local/bin/composer

WORKDIR /code

# Initialize
COPY . /code/
RUN composer install --no-interaction

CMD php ./src/run.php --data=/data