using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.AspNetCore.SignalR;
using NetTopologySuite.IO.Converters;
using Npgsql;
using PostgresForDotnetDev.Api;
using PostgresForDotnetDev.Api.Core;
using JsonOptions = Microsoft.AspNetCore.Http.Json.JsonOptions;

#pragma warning disable CS0618
NpgsqlConnection.GlobalTypeMapper.UseNetTopologySuite(geographyAsDefault: true);

#pragma warning restore CS0618

var builder = WebApplication.CreateBuilder(args);

void Configure(JsonSerializerOptions serializerOptions)
{
    serializerOptions.WriteIndented = true;
    serializerOptions.Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping;
    serializerOptions.PropertyNamingPolicy = JsonNamingPolicy.CamelCase;
    serializerOptions.NumberHandling = JsonNumberHandling.AllowNamedFloatingPointLiterals;

    serializerOptions.Converters.Add(new GeoJsonConverterFactory());
    serializerOptions.Converters.Add(new JsonStringEnumConverter());
}

builder.Services
    .AddEndpointsApiExplorer()
    .AddSwaggerGen();

builder.Services.Configure<JsonOptions>(o => Configure(o.SerializerOptions));
builder.Services.Configure<Microsoft.AspNetCore.Mvc.JsonOptions>(o => Configure(o.JsonSerializerOptions));

// Add Postgres Subscription

builder.Services.AddHostedService(serviceProvider =>
    {
        var logger =
            serviceProvider.GetRequiredService<ILogger<BackgroundWorker>>();
        var hubContext =
            serviceProvider.GetRequiredService<IHubContext<FleetManagementHub>>();

        return new BackgroundWorker(
            logger,
            ct => FuelEfficiencyAlertsPostgresSubscription.SubscribeAsync(
                builder.Configuration.GetConnectionString("Postgres")!,
                hubContext,
                ct
            )
        );
    }
);

// Add SignalR
builder.Services.AddSignalR();

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.UseSwagger()
        .UseSwaggerUI();
}

app.MapHub<FleetManagementHub>("/fleet-management-hub");

app.Run();
