namespace PostgresForDotnetDev.Api.FleetManagement;

public record FuelEfficiencyAlert(
    int VehicleId,
    DateTimeOffset StartTime,
    DateTimeOffset EndTime,
    decimal FuelEfficiency
);
