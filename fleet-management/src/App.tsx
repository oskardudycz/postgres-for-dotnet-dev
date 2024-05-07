import "./tailwind.css";

import './App.css';
import { useEffect, useState } from "react";
import { HttpTransportType, HubConnectionBuilder, LogLevel } from "@microsoft/signalr";

type FuelEfficiencyAlert = {
  vehicleId: number;
  startTime: Date;
  endTime: Date;
  fuelEfficiency: number;
};

function FleetManagementApp() {
  const [alerts, setAlerts] = useState<FuelEfficiencyAlert[]>([]);

  useEffect(() => {
    const connection = new HubConnectionBuilder()
      .configureLogging(LogLevel.Debug)
      .withUrl("http://localhost:5000/hubs/fleet-management", {
        skipNegotiation: true,
        transport: HttpTransportType.WebSockets
      })
      .withAutomaticReconnect()
      .build();

    connection.on("fuelefficiencyalertraised", (alert: FuelEfficiencyAlert) => {
      alert.startTime = new Date(alert.startTime);
      alert.endTime = new Date(alert.endTime);
      setAlerts((prevAlerts) => [...prevAlerts, alert]);
    });

    connection.start().catch((err) => console.error(err));

    return () => {
      connection.stop();
    };
  }, []);

  return (
    <div className="mx-auto max-w-5xl px-6 py-4">
      <h1 className="text-3xl font-bold mb-4">Fleet Management App</h1>
      {alerts.length === 0 ? (
        <div className="text-lg">There are no alerts at the moment.</div>
      ) : (
        <div className="grid grid-cols-3 gap-4">
          {alerts.map((alert) => (
            <div
              key={`${alert.vehicleId}-${alert.startTime.toISOString()}`}
              className="bg-white rounded-lg shadow p-4"
            >
              <div className="text-lg font-bold mb-2">
                <img src="alert.gif" alt="alert" />
                Alert for Vehicle {alert.vehicleId}
              </div>
              <div className="text-sm mb-2">
                Start Time: {alert.startTime.toLocaleString()}
              </div>
              <div className="text-sm mb-2">
                End Time: {alert.endTime.toLocaleString()}
              </div>
              <div className="text-sm">Fuel Efficiency: {alert.fuelEfficiency}</div>
            </div>
          ))}
        </div>
      )}
    </div>
  );  
}

export default FleetManagementApp;
