using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Json.Serialization;
using NetTopologySuite.Geometries;
using NetTopologySuite.IO.Converters;
using Npgsql;
using PostgresForDotnetDev.CLI;
using PostgresOutbox.Serialization;
using PostgresOutbox.Subscriptions;
using PostgresOutbox.Subscriptions.Replication;

#pragma warning disable CS0618
NpgsqlConnection.GlobalTypeMapper.UseNetTopologySuite();
#pragma warning restore CS0618

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

var slotName = "trips_slot" + Guid.NewGuid().ToString().Replace("-", "");

var dataMapper = new FlatObjectMapper<TripRecord>(NameTransformations.FromPostgres);

var subscriptionOptions =
    new SubscriptionOptions(Settings.ConnectionString, slotName, "events_pub", "trips", dataMapper);
var subscription = new Subscription();

await foreach (var readEvent in subscription.Subscribe(subscriptionOptions, ct: ct))
{
    Console.WriteLine(JsonSerialization.ToJson(readEvent,serializerOptions));
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
