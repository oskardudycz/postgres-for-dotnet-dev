FROM mcr.microsoft.com/dotnet/aspnet:7.0 AS base
WORKDIR /app
EXPOSE 80
EXPOSE 443

FROM mcr.microsoft.com/dotnet/sdk:7.0 AS build
WORKDIR /src
COPY ["PostgresForDotnetDev.Api/PostgresForDotnetDev.Api.csproj", "PostgresForDotnetDev.Api/"]
RUN dotnet restore "PostgresForDotnetDev.Api/PostgresForDotnetDev.Api.csproj"
COPY . .
WORKDIR "/src/PostgresForDotnetDev.Api"
RUN dotnet build "PostgresForDotnetDev.Api.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "PostgresForDotnetDev.Api.csproj" -c Release -o /app/publish

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "PostgresForDotnetDev.Api.dll"]
