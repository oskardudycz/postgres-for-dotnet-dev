namespace PostgresForDotnetDev.Api.FleetManagement;

public record FuelEfficiencyAlert(
    int VehicleId,
    DateTime StartTime,
    DateTime EndTime,
    decimal FuelEfficiency
);

