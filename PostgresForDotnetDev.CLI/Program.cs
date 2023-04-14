using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Json.Serialization;
using NetTopologySuite.Geometries;
using NetTopologySuite.IO.Converters;
using Npgsql;
using PostgresForDotnetDev.CLI;
using PostgresForDotnetDev.Core;
using PostgresOutbox.Serialization;
using PostgresOutbox.Subscriptions;
using PostgresOutbox.Subscriptions.Replication;

#pragma warning disable CS0618
NpgsqlConnection.GlobalTypeMapper.UseNetTopologySuite(geographyAsDefault: true);

#pragma warning restore CS0618

await using var connection = new NpgsqlConnection(Settings.ConnectionString);

const bool shouldSetup = false;

if (shouldSetup)
#pragma warning disable CS0162
{
/////////////////////////////
// 1. Trips
/////////////////////////////
    connection.Run(@"
    DROP TABLE IF EXISTS trips CASCADE;
    CREATE TABLE trips (
        trip_time TIMESTAMP NOT NULL,
        vehicle_id INT NOT NULL,
        driver_name VARCHAR(255) NOT NULL,
        start_location TEXT NOT NULL,
        end_location TEXT NOT NULL,
        distance_miles NUMERIC(10,2) NOT NULL,
        fuel_used_gallons NUMERIC(10,2) NOT NULL,
        PRIMARY KEY (trip_time, vehicle_id)
    );

    SELECT create_hypertable('trips', 'trip_time');

    INSERT INTO trips (trip_time, vehicle_id, driver_name, start_location, end_location, distance_miles, fuel_used_gallons)
    VALUES
    ('2023-04-03 08:00:00', 1, 'John Doe', '52.292064, 21.036320', '52.156574, 19.133474', 200, 10),
    ('2023-04-03 10:00:00', 1, 'Jane Smith', '52.156574, 19.133474', '52.292064, 21.036320', 200, 10),
    ('2023-04-04 08:00:00', 2, 'John Doe', '52.156574, 19.133474', '50.890716, 21.951678', 100, 5),
    ('2023-04-04 10:00:00', 2, 'Jane Smith', '50.890716, 21.951678', '52.156574, 19.133474', 100, 5),
    ('2023-04-05 08:00:00', 3, 'Jane Doe', '52.156574, 19.133474', '52.223045, 20.971662', 1, 10),
    ('2023-04-05 10:00:00', 3, 'Jane Doe', '52.223045, 20.971662', '52.156574, 19.133474', 1, 10),
    ('2023-04-06 08:00:00', 4, 'John Smith', '52.156574, 19.133474', '52.223045, 20.971662', 1, 15),
    ('2023-04-06 10:00:00', 4, 'John Smith', '52.223045, 20.971662', '52.156574, 19.133474', 1, 15);
");

// Find the total distance traveled and fuel used for each vehicle:
    await connection.PrintAsync(@"
    SELECT vehicle_id, SUM(distance_miles) AS total_distance, SUM(fuel_used_gallons) AS total_fuel
    FROM trips
    GROUP BY vehicle_id;
");

// Find the average distance and fuel efficiency for each driver:
    await connection.PrintAsync(@"
    SELECT driver_name, AVG(distance_miles) AS avg_distance, SUM(distance_miles)/SUM(fuel_used_gallons) AS fuel_efficiency
    FROM trips
    GROUP BY driver_name;
");

/////////////////////////////////////////////////////
// 2. Materialized rolling view from TimescaleDB
////////////////////////////////////////////////////

// Calculate the average fuel efficiency for each vehicle over a rolling window of 30 days
    connection.Run("DROP MATERIALIZED VIEW IF EXISTS vehicle_fuel_efficiency_avg;");
    connection.Run(@"
    CREATE MATERIALIZED VIEW vehicle_fuel_efficiency_avg
    WITH (timescaledb.continuous) AS
    SELECT time_bucket('1 day', trip_time) AS bucket,
           vehicle_id,
           AVG(distance_miles)/AVG(fuel_used_gallons) AS fuel_efficiency_avg
    FROM trips
    WHERE trip_time >= now() - INTERVAL '30 days'
    GROUP BY bucket, vehicle_id;
");
    connection.Run(@"
    SELECT add_continuous_aggregate_policy(
      continuous_aggregate => 'vehicle_fuel_efficiency_avg',
      start_offset => INTERVAL '3 days',
      end_offset => INTERVAL '1 second',
      schedule_interval => INTERVAL '1 day');
");
    connection.Run(@"
    CALL refresh_continuous_aggregate(
        continuous_aggregate => 'vehicle_fuel_efficiency_avg',
        window_start => (now() - INTERVAL '1 week')::TIMESTAMP,
        window_end => now()::TIMESTAMP
    );
");

// Create a table to store alerts for vehicles with low fuel efficiency
    connection.Run(@"
    DROP TABLE IF EXISTS fuel_efficiency_alerts;
    CREATE TABLE fuel_efficiency_alerts (
        vehicle_id INT NOT NULL,
        start_time TIMESTAMP NOT NULL,
        end_time TIMESTAMP NOT NULL,
        fuel_efficiency NUMERIC(10,2) NOT NULL,
        PRIMARY KEY (vehicle_id, start_time)
    );
");

// Create a table to store alerts for vehicles with low fuel efficiency
    connection.Run(@"
    CREATE OR REPLACE FUNCTION check_fuel_efficiency_and_insert_alerts(p_job_id INTEGER, p_config JSONB)
    RETURNS VOID AS $$
    BEGIN
      INSERT INTO
        fuel_efficiency_alerts (
          vehicle_id,
          start_time,
          end_time,
          fuel_efficiency
        )
      SELECT
        vehicle_id,
        bucket AS start_time,
        bucket + INTERVAL '1 day' AS end_time,
        fuel_efficiency_avg AS fuel_efficiency
      FROM
        vehicle_fuel_efficiency_avg
      WHERE
        fuel_efficiency_avg < 5
      ON CONFLICT (vehicle_id, start_time) DO UPDATE
        SET
          fuel_efficiency = EXCLUDED.fuel_efficiency,
          end_time = EXCLUDED.end_time;

      DELETE FROM
        fuel_efficiency_alerts AS a
      WHERE
        NOT EXISTS (
          SELECT 1
          FROM
            vehicle_fuel_efficiency_avg AS f
          WHERE
            a.vehicle_id = f.vehicle_id
            AND a.start_time = f.bucket
            AND f.fuel_efficiency_avg < 5
        );
    END;
    $$ LANGUAGE plpgsql;

");

    connection.Run(@"
    CREATE OR REPLACE FUNCTION remove_job_if_exists(p_function_name TEXT)
    RETURNS VOID AS $$
    DECLARE
        v_job_id INTEGER;
    BEGIN
        SELECT job_id INTO v_job_id
        FROM timescaledb_information.jobs
        WHERE proc_name = p_function_name;

        IF FOUND THEN
            PERFORM delete_job(v_job_id);
        END IF;
        RETURN;
    END;
    $$ LANGUAGE plpgsql;

    SELECT remove_job_if_exists('check_fuel_efficiency_and_insert_alerts');
    SELECT add_job('check_fuel_efficiency_and_insert_alerts', '1 second');
");

    await Task.Delay(TimeSpan.FromSeconds(2));

// Generate a report that shows the fuel efficiency for each vehicle over time, as well as any alerts that have been generated
    await connection.PrintAsync(@"
    SELECT trips.vehicle_id,
           trips.trip_time,
           trips.distance_miles/trips.fuel_used_gallons AS fuel_efficiency,
           fuel_efficiency_alerts.start_time,
           fuel_efficiency_alerts.end_time
    FROM trips
    LEFT JOIN vehicle_fuel_efficiency_avg ON trips.vehicle_id = vehicle_fuel_efficiency_avg.vehicle_id
                                            AND time_bucket('1 day', trips.trip_time) = vehicle_fuel_efficiency_avg.bucket
    LEFT JOIN fuel_efficiency_alerts ON trips.vehicle_id = fuel_efficiency_alerts.vehicle_id
    WHERE trips.trip_time >= now() - INTERVAL '60 days'
    ORDER BY trips.vehicle_id, trips.trip_time;
");

/////////////////////////////////////////////////////
// 3. Let's add PostGis
////////////////////////////////////////////////////

    connection.Run(@"CREATE EXTENSION IF NOT EXISTS postgis;");

    connection.Run(@"
    ALTER TABLE trips
    ADD COLUMN route GEOMETRY(LINESTRING, 4326) NULL;

    UPDATE trips
    SET route = ST_Transform(
        ST_MakeLine(
            ST_GeomFromText(
              'POINT(' ||
              split_part(start_location, ',', 1) || ' ' ||
              split_part(start_location, ',', 2) || ')',
              4326
            ),
            ST_GeomFromText(
              'POINT(' ||
              split_part(end_location, ',', 1) || ' ' ||
              split_part(end_location, ',', 2) || ')',
              4326
            )
        ), 4326)
    WHERE route IS NULL;

    ALTER TABLE trips
    ALTER COLUMN route SET NOT NULL;
");

    connection.Run("DROP MATERIALIZED VIEW IF EXISTS vehicle_fuel_efficiency_avg;");

    connection.Run(@"
    ALTER TABLE trips
        DROP COLUMN distance_miles,
        ADD COLUMN distance_miles NUMERIC(10, 2) GENERATED ALWAYS AS (
            ST_Length(route::geography)/1609.34
        ) STORED;
");

    connection.Run(@"
    ALTER TABLE trips
        DROP COLUMN start_location,
        ADD COLUMN start_location GEOMETRY(POINT, 4326) GENERATED ALWAYS AS (
            ST_StartPoint(route)
        ) STORED;
");

    connection.Run(@"
    ALTER TABLE trips
        DROP COLUMN end_location,
        ADD COLUMN end_location GEOMETRY(POINT, 4326) GENERATED ALWAYS AS (
            ST_EndPoint(route)
        ) STORED;
");

    connection.Run(@"
    CREATE MATERIALIZED VIEW vehicle_fuel_efficiency_avg
    WITH (timescaledb.continuous) AS
    SELECT time_bucket('1 day', trip_time) AS bucket,
           vehicle_id,
           AVG(distance_miles)/AVG(fuel_used_gallons) AS fuel_efficiency_avg
    FROM trips
    WHERE trip_time >= now() - INTERVAL '30 days'
    GROUP BY bucket, vehicle_id;
");
    connection.Run(@"
    CALL refresh_continuous_aggregate(
        continuous_aggregate => 'vehicle_fuel_efficiency_avg',
        window_start => (now() - INTERVAL '1 week')::TIMESTAMP,
        window_end => now()::TIMESTAMP
    );
");


// Now it looks as:
// CREATE TABLE trips (
//     trip_time TIMESTAMP NOT NULL,
//     vehicle_id INT NOT NULL,
//     driver_name VARCHAR(255) NOT NULL,
//     start_location GEOMETRY(POINT, 4326) GENERATED ALWAYS AS (ST_StartPoint(route) ) STORED,
//     end_location GEOMETRY(POINT, 4326) GENERATED ALWAYS AS (ST_EndPoint(route) ) STORED,
//     distance_miles NUMERIC(10, 2) GENERATED ALWAYS AS (ST_Length(route::geography)/1609.34) STORED,
//     fuel_used_gallons NUMERIC(10,2) NOT NULL,
//     route GEOMETRY(LINESTRING, 4326) NULL,
//     PRIMARY KEY (trip_time, vehicle_id)
// );

    connection.Run(@"
    INSERT INTO trips (trip_time, vehicle_id, driver_name, route, fuel_used_gallons)
    VALUES
    ('2023-04-12 14:30:01', 12345, 'John Doe', 'SRID=4326;LINESTRING(-74.0060 40.7128, -73.9352 40.7306, -73.8701 40.6655)',  2.5);
");

    await connection.PrintAsync(@"
    SELECT trips.vehicle_id,
           trips.trip_time,
           trips.distance_miles/trips.fuel_used_gallons AS fuel_efficiency,
           fuel_efficiency_alerts.start_time,
           fuel_efficiency_alerts.end_time
    FROM trips
    LEFT JOIN vehicle_fuel_efficiency_avg ON trips.vehicle_id = vehicle_fuel_efficiency_avg.vehicle_id
                                            AND time_bucket('1 day', trips.trip_time) = vehicle_fuel_efficiency_avg.bucket
    LEFT JOIN fuel_efficiency_alerts ON trips.vehicle_id = fuel_efficiency_alerts.vehicle_id
    WHERE trips.trip_time >= now() - INTERVAL '60 days'
    ORDER BY trips.vehicle_id, trips.trip_time;
");

    await connection.PrintAsync(@"
    SELECT column_name, CASE WHEN data_type = 'USER-DEFINED' THEN udt_name ELSE data_type END as data_type,  is_nullable, udt_name, generation_expression
    FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'trips';
");
}
#pragma warning restore CS0162

var serializerOptions = new JsonSerializerOptions
{
    WriteIndented = true,
    Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
    PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    Converters = { new GeoJsonConverterFactory() },
    NumberHandling = JsonNumberHandling.AllowNamedFloatingPointLiterals
};

var cancellationTokenSource = new CancellationTokenSource();

var ct = cancellationTokenSource.Token;

const string slotName = "trips_slot";

var dataMapper = new FlatObjectMapper<TripRecord>(NameTransformations.FromPostgres);

var subscriptionOptions =
    new SubscriptionOptions(Settings.ConnectionString, slotName, "fuel_efficiency_alerts_pub", "fuel_efficiency_alerts",
        dataMapper, CreateStyle.WhenNotExists);
var subscription = new Subscription();

await foreach (var readEvent in subscription.Subscribe(subscriptionOptions, ct: ct))
{
    Console.WriteLine(JsonSerialization.ToJson(readEvent, serializerOptions));
}


record TripRecord(
    DateTime TripTime,
    int VehicleId,
    string DriverName,
    decimal FuelUsedGallons,
    Geometry Route,
    decimal? DistanceMiles,
    Geometry? StartLocation,
    Geometry? EndLocation
);
