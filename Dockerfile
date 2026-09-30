# Bookworm rather than trixie: Microsoft publishes msodbcsql18 / mssql-tools for Debian 12, and its
# Debian 13 repository cannot be verified by trixie's apt. Bullseye itself can no longer be built --
# its security pool is being archived (packages still listed in the index return 404).
FROM php:8.5-cli-bookworm

ARG COMPOSER_FLAGS="--prefer-dist --no-interaction"
ARG DEBIAN_FRONTEND=noninteractive
ENV COMPOSER_ALLOW_SUPERUSER 1
ENV COMPOSER_PROCESS_TIMEOUT 3600

WORKDIR /code/

COPY docker/php-prod.ini /usr/local/etc/php/php.ini
COPY docker/composer-install.sh /tmp/composer-install.sh

# Install dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    locales \
    unzip \
    ssh \
    apt-transport-https \
    wget \
    libxml2-dev \
    libicu-dev \
    gnupg2 \
    unixodbc \
    unixodbc-dev \
    libgss3 \
    # apt-key is deprecated since bookworm -- the key goes to its own keyring referenced by signed-by
    && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
    && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" \
        > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    # mssql-tools stays on the 17.x line (17.11.1.1, the same build the bullseye image installed): the
    # 18.x bcp and sqlcmd default to mandatory encryption, which would change how every export connects

    && ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18 mssql-tools \
    && rm -r /var/lib/apt/lists/* \
    && sed -i 's/^# *\(en_US.UTF-8\)/\1/' /etc/locale.gen \
    && locale-gen \
    && chmod +x /tmp/composer-install.sh \
    && /tmp/composer-install.sh

RUN docker-php-ext-configure intl \
    && docker-php-ext-install intl

ENV LANGUAGE=en_US.UTF-8
ENV LANG=en_US.UTF-8
ENV LC_ALL=en_US.UTF-8

# PDO mssql
# 5.13 is the first line that supports PHP > 8.2 (it requires >= 8.3)
RUN pecl install pdo_sqlsrv-5.13.3 sqlsrv-5.13.3 \
    && docker-php-ext-enable sqlsrv pdo_sqlsrv \
    && docker-php-ext-install xml

# Set path
ENV PATH $PATH:/opt/mssql-tools/bin

# Keep SSL compatible with older servers, as the bullseye image was
# (MinProtocol = TLSv1 and CipherString = DEFAULT@SECLEVEL=1 under OpenSSL 1.1.1).
# Bookworm's openssl.cnf has no system_default section, so the section is created rather than edited --
# a sed on lines that do not exist would silently change nothing. The values are the OpenSSL 3.0
# equivalents of the old ones: TLS 1.0/1.1 sign with SHA-1, which 3.0 only permits at security level 0,
# and 3.0 no longer connects to servers lacking RFC 5746 secure renegotiation unless told to, which
# 1.1.1 did by default.
RUN grep -q '^\[openssl_init\]' /etc/ssl/openssl.cnf \
    && ! grep -q '^\[system_default_sect\]' /etc/ssl/openssl.cnf \
    && sed -i 's/^\[openssl_init\]$/[openssl_init]\nssl_conf = ssl_sect/' /etc/ssl/openssl.cnf \
    && printf '\n[ssl_sect]\nsystem_default = system_default_sect\n\n[system_default_sect]\nMinProtocol = TLSv1\nCipherString = DEFAULT@SECLEVEL=0\nOptions = UnsafeLegacyServerConnect\n' \
        >> /etc/ssl/openssl.cnf

## Composer - deps always cached unless changed
# First copy only composer files
COPY composer.* /code/

# Download dependencies, but don't run scripts or init autoloaders as the app is missing
RUN composer install $COMPOSER_FLAGS --no-scripts --no-autoloader

# Copy rest of the app
COPY . /code/

# Run normal composer - all deps are cached already
RUN composer install $COMPOSER_FLAGS

CMD php ./src/run.php
