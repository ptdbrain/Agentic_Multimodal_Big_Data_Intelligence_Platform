#!/usr/bin/env bash
set -e

service postgresql start
sed -i "s/#listen_addresses = 'localhost'/listen_addresses = '*'/" /etc/postgresql/16/main/postgresql.conf
grep -q "host all all 0.0.0.0/0 md5" /etc/postgresql/16/main/pg_hba.conf || echo "host all all 0.0.0.0/0 md5" >> /etc/postgresql/16/main/pg_hba.conf
grep -q "host all all ::0/0 md5" /etc/postgresql/16/main/pg_hba.conf || echo "host all all ::0/0 md5" >> /etc/postgresql/16/main/pg_hba.conf
service postgresql restart

sudo -u postgres psql -tc "SELECT 1 FROM pg_roles WHERE rolname='sentinel'" | grep -q 1 || sudo -u postgres psql -c "CREATE USER sentinel WITH PASSWORD 'sentinelpass' SUPERUSER;"
sudo -u postgres psql -tc "SELECT 1 FROM pg_database WHERE datname='sentinel_dw'" | grep -q 1 || sudo -u postgres psql -c "CREATE DATABASE sentinel_dw OWNER sentinel;"

echo "PostgreSQL setup complete!"
