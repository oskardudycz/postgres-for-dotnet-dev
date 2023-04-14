namespace PostgresForDotnetDev.Api;

public record FuelEfficiencyAlert(
    int VehicleId,
    DateTime StartTime,
    DateTime EndTime,
    decimal FuelEfficiency
);
