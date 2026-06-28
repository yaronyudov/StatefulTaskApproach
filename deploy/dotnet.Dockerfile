# Shared multi-stage build for the C# services. Pass PROJECT and DLL as build args.
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
ARG PROJECT
WORKDIR /src
COPY . .
RUN dotnet publish src/${PROJECT}/${PROJECT}.csproj -c Release -o /app

FROM mcr.microsoft.com/dotnet/aspnet:8.0
ARG DLL
WORKDIR /app
COPY --from=build /app .
ENV DLL=${DLL}
ENTRYPOINT ["sh", "-c", "dotnet ${DLL}"]
