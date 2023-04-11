using System.Text;

namespace PostgresForDotnetDev.TimescaleDB;

public static class TimeSpanFomatter
{
    public static string FormatToTimescaleDB(this TimeSpan timeSpan)
    {
        var builder = new StringBuilder();

        void AppendIfNonZero(int value, string unit)
        {
            if (value > 0)
            {
                builder.Append($"{value} {unit}{(value > 1 ? "s" : "")} ");
            }
        }

        AppendIfNonZero(timeSpan.Days, "day");
        AppendIfNonZero(timeSpan.Hours, "hour");
        AppendIfNonZero(timeSpan.Minutes, "minute");
        AppendIfNonZero(timeSpan.Seconds, "second");

        return builder.ToString().TrimEnd();
    }
}
