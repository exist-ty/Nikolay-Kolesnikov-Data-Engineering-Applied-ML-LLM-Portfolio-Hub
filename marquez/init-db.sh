#!/bin/bash
# Взято дословно из github.com/MarquezProject/marquez/blob/main/docker/init-db.sh —
# официальный docker-compose.yml Marquez монтирует volume db-init как
# ПУСТОЙ, а этот скрипт (создающий роль/базу marquez) подставляется
# отдельно и в их репозитории не лежит рядом с compose-файлом. Без него
# marquez-api падает при старте: FATAL: password authentication failed
# for user "marquez" — marquez.dev.yml (конфиг внутри образа api) жёстко
# использует user=marquez/password=marquez, не читая POSTGRES_USER/PASSWORD
# из окружения самого api.
set -eu

psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER}" > /dev/null <<-EOSQL
  CREATE USER ${MARQUEZ_USER};
  ALTER USER ${MARQUEZ_USER} WITH PASSWORD '${MARQUEZ_PASSWORD}';
  CREATE DATABASE ${MARQUEZ_DB};
  GRANT ALL PRIVILEGES ON DATABASE ${MARQUEZ_DB} TO ${MARQUEZ_USER};
EOSQL
