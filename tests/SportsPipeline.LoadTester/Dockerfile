FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src
COPY Directory.*.props ./
COPY global.json ./
COPY src/SportsPipeline.Contracts/ src/SportsPipeline.Contracts/
COPY src/SportsPipeline.Domain/ src/SportsPipeline.Domain/
COPY tests/SportsPipeline.LoadTester/ tests/SportsPipeline.LoadTester/
RUN dotnet publish tests/SportsPipeline.LoadTester/SportsPipeline.LoadTester.csproj -c Release -o /app

FROM mcr.microsoft.com/dotnet/runtime:8.0
WORKDIR /app
COPY --from=build /app .
ENTRYPOINT ["dotnet", "SportsPipeline.LoadTester.dll"]
