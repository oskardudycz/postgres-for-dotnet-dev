FROM postgis/postgis:15-3.3
RUN apt-get update \
 && apt install -y gnupg postgresql-common apt-transport-https lsb-release wget

RUN echo "deb https://packagecloud.io/timescale/timescaledb/debian/ $(lsb_release -c -s) main" | tee /etc/apt/sources.list.d/timescaledb.list

RUN wget --quiet -O - https://packagecloud.io/timescale/timescaledb/gpgkey | apt-key add -

RUN apt update

RUN apt install -y timescaledb-2-postgresql-15

# RUN timescaledb-tune --quiet --yes --dry-run > /var/lib/postgresql/data/postgresql.conf

COPY updateConfig.sh      /docker-entrypoint-initdb.d/_updateConfig.sh
