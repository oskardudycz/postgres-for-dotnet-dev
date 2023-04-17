using Microsoft.AspNetCore.SignalR;

namespace PostgresForDotnetDev.Api.FleetManagement;

public class FleetManagementHub : Hub
{
    public static Task SendFuelEfficiencyAlert(IHubContext<FleetManagementHub> hubContext, FuelEfficiencyAlert alert) =>
        hubContext.Clients.All.SendAsync("FuelEfficiencyAlertRaised", alert);
}
