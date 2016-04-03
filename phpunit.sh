#!/usr/bin/env bash

composer selfupdate
composer install -n

waitforservices

./vendor/bin/phpunit "$@"
