#!/bin/bash

#cat  "/var/lib/postgresql/data/postgresql.conf"
#echo "replacing"

echo "shared_preload_libraries = 'timescaledb'" >> /var/lib/postgresql/data/postgresql.conf
echo "listen_addresses = '*'" >> /var/lib/postgresql/data/postgresql.conf
echo "wal_level=logical" >> /var/lib/postgresql/data/postgresql.conf

#echo "replaced"
#cat  "/var/lib/postgresql/data/postgresql.conf"


psql -U postgres -c 'CREATE EXTENSION IF NOT EXISTS timescaledb;'
