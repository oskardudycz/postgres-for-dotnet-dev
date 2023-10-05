using Microsoft.AspNetCore.SignalR;
using PostgresOutbox.Subscriptions;
using PostgresOutbox.Subscriptions.Management;
using PostgresOutbox.Subscriptions.Replication;
using static PostgresForDotnetDev.Api.FleetManagement.FleetManagementHub;

namespace PostgresForDotnetDev.Api.FleetManagement;

public class FuelEfficiencyAlertsPostgresSubscription
{
    public static async Task SubscribeAsync(
        string connectionString,
        IHubContext<FleetManagementHub> hubContext,
        CancellationToken ct
    )
    {
        const string slotName = "fuel_efficiency_alerts_slot";
        var createStyle = CreateStyle.WhenNotExists;

        var dataMapper = new FlatObjectMapper<FuelEfficiencyAlert>(NameTransformations.FromPostgres);

        var subscriptionOptions = new SubscriptionOptions(
            connectionString,
            new PublicationManagement.PublicationSetupOptions(
                "fuel_efficiency_alerts_pub",
                "fuel_efficiency_alerts",
                createStyle,
                true
            ),
            new ReplicationSlotManagement.ReplicationSlotSetupOptions(
                slotName,
                CreateStyle.AlwaysRecreate
            ),
            dataMapper
        );

        var subscription = new Subscription();

        await foreach (var alert in subscription.Subscribe(subscriptionOptions, ct: ct))
        {
            await SendFuelEfficiencyAlert(hubContext, (FuelEfficiencyAlert)alert);
        }
    }
}
