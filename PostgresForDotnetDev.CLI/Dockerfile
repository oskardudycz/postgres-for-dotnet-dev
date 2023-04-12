FROM mcr.microsoft.com/dotnet/runtime:7.0 AS base
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:7.0 AS build
WORKDIR /src
COPY ["PostgresForDotnetDev.CLI/PostgresForDotnetDev.CLI.csproj", "PostgresForDotnetDev.CLI/"]
RUN dotnet restore "PostgresForDotnetDev.CLI/PostgresForDotnetDev.CLI.csproj"
COPY . .
WORKDIR "/src/PostgresForDotnetDev.CLI"
RUN dotnet build "PostgresForDotnetDev.CLI.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "PostgresForDotnetDev.CLI.csproj" -c Release -o /app/publish

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "PostgresForDotnetDev.CLI.dll"]
