using Microsoft.AspNetCore.SignalR;
using PostgresOutbox.Subscriptions;
using PostgresOutbox.Subscriptions.Replication;

namespace PostgresForDotnetDev.Api;

public class FuelEfficiencyAlertsPostgresSubscription
{
    public static async Task SubscribeAsync(
        string connectionString,
        IHubContext<FleetManagementHub> hubContext,
        CancellationToken ct
    )
    {
        const string slotName = "fuel_efficiency_alerts_slot";

        var dataMapper = new FlatObjectMapper<FuelEfficiencyAlert>(NameTransformations.FromPostgres);

        var subscriptionOptions = new SubscriptionOptions(
            connectionString,
            slotName,
            "fuel_efficiency_alerts_pub",
            "fuel_efficiency_alerts",
            dataMapper,
            CreateStyle.WhenNotExists
        );

        var subscription = new Subscription();

        await foreach (var alert in subscription.Subscribe(subscriptionOptions, ct: ct))
        {
           await FleetManagementHub.SendFuelEfficiencyAlert(hubContext, (FuelEfficiencyAlert) alert);
        }
    }
}
