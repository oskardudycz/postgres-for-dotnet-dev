CREATE EXTENSION IF NOT EXISTS timescaledb;


CREATE TABLE fuel_efficiency_alerts (
   vehicle_id INT NOT NULL,
   start_time TIMESTAMPTZ NOT NULL,
   end_time TIMESTAMPTZ NOT NULL,
   fuel_efficiency NUMERIC(10,2) NOT NULL,
   PRIMARY KEY (vehicle_id, start_time)
);

---------------------------
INSERT INTO trips (trip_time, vehicle_id, driver_name, route, fuel_used_liters)
VALUES
    ((NOW() - interval '1 days'), cast(extract(epoch from current_timestamp) as INTEGER), 'John Doe', 'SRID=4326;LINESTRING(-74.0060 40.7128, -73.9352 40.7306, -73.8701 40.6655)',  5.5);
select * from trips;
SELECT * FROM fuel_efficiency_alerts;

SELECT sent_lsn, write_lsn FROM pg_stat_replication;
SELECT pg_current_wal_lsn();

SELECT name, setting
FROM pg_settings
WHERE name = 'max_logical_replication_workers';

SELECT * FROM pg_replication_slots;
SELECT * FROM pg_stat_replication;

BEGIN ;
insert into fuel_efficiency_alerts (vehicle_id, start_time, end_time, fuel_efficiency)
values (cast(extract(epoch from current_timestamp) as INTEGER),current_TIMESTAMPTZ,current_TIMESTAMPTZ,'3.00');
COMMIT;

SELECT * FROM fuel_efficiency_alerts;


------------------
-- jobs statuses
------------------
SELECT
    j.job_id,
    j.schedule_interval,
    s.last_run_started_at,
    s.last_run_status,
    s.total_runs,
    s.total_successes,
    s.total_failures
FROM timescaledb_information.jobs j
         JOIN timescaledb_information.job_stats s ON j.job_id = s.job_id
WHERE s.total_runs > 0;

--------------------------
-- list hypertables
---------------------------
SELECT
    *
FROM timescaledb_information.hypertables;
