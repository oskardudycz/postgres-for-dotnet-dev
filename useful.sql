select Count(1) from trips;

INSERT INTO trips (trip_time, vehicle_id, driver_name, route, fuel_used_gallons)
VALUES
    ('2023-04-22 14:33:00', 12345, 'John Doe', 'SRID=4326;LINESTRING(-74.0060 40.7128, -73.9352 40.7306, -73.8701 40.6655)',  2.5);

SELECT sent_lsn, write_lsn FROM pg_stat_replication;
SELECT pg_current_wal_lsn();

SELECT name, setting
FROM pg_settings
WHERE name = 'max_logical_replication_workers';

SELECT * FROM pg_replication_slots;
SELECT * FROM pg_stat_replication;
SELECT * FROM pg_stat_subscription;

BEGIN ;
insert into fuel_efficiency_alerts (vehicle_id, start_time, end_time, fuel_efficiency)
values (11345,'2023-04-14 19:27:19.000000','2023-04-14 19:27:29.000000','3.00');
COMMIT;

CREATE TABLE fuel_efficiency_alerts (
     vehicle_id INT NOT NULL,
     start_time TIMESTAMP NOT NULL,
     end_time TIMESTAMP NOT NULL,
     fuel_efficiency NUMERIC(10,2) NOT NULL,
     PRIMARY KEY (vehicle_id, start_time)
);
