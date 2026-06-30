param(
    [int]$TotalEvents = 100000,
    [int]$UniqueMatches = 5000,
    [int]$Partitions = 12
)

$ErrorActionPreference = "Stop"
$RootPath = Resolve-Path "$PSScriptRoot\..\.."
Set-Location $RootPath

function Run-Benchmark {
    param(
        [string]$Engine,
        [string]$ConsumerGroup
    )
    
    Write-Host "`n==================================================" -ForegroundColor Cyan
    Write-Host " STARTING $Engine BENCHMARK" -ForegroundColor Cyan
    Write-Host "==================================================" -ForegroundColor Cyan
    
    # 1. Clean Slate
    Write-Host "[1/5] Tearing down and wiping volumes for a clean slate..." -ForegroundColor Yellow
    docker compose -f deploy/docker-compose.yml --profile orleans --profile flink down -v --remove-orphans | Out-Null
    try {
        docker rm -f sports-pipeline-redpanda-1 sports-pipeline-opensearch-1 sports-pipeline-mongo-1 sports-pipeline-redis-1 sports-pipeline-classifier-orleans-1 sports-pipeline-jobmanager-1 sports-pipeline-taskmanager-1 2>&1 | Out-Null
    } catch {}
    Start-Sleep -Seconds 5
    
    # 2. Start infra
    Write-Host "[2/5] Starting Infrastructure..." -ForegroundColor Yellow
    if ($Engine -eq "Orleans") {
        docker compose -f deploy/docker-compose.yml up -d redpanda opensearch mongo | Out-Null
    }
    else {
        docker compose -f deploy/docker-compose.yml up -d redpanda opensearch mongo jobmanager taskmanager | Out-Null
        Write-Host "      Waiting for Flink TaskManager to be ready..." -ForegroundColor Yellow
        Start-Sleep -Seconds 10
    }
    
    Write-Host "      Waiting for Kafka..." -ForegroundColor Yellow
    Start-Sleep -Seconds 5

    # 2b. Pre-create the topic with N partitions BEFORE any producer runs, so matches fan out across
    #     partitions (the producer would otherwise auto-create it with a single partition).
    Write-Host "      Creating topic 'validated-events' with $Partitions partitions..." -ForegroundColor Yellow
    docker compose -f deploy/docker-compose.yml exec -T redpanda rpk topic create validated-events -p $Partitions 2>&1 | Out-Null

    # 3. Inject Data
    Write-Host "[3/5] Injecting $TotalEvents events using LoadTester container..." -ForegroundColor Yellow
    docker build -t loadtester -f tests/SportsPipeline.LoadTester/Dockerfile . | Out-Host
    docker run --rm --network sports-pipeline_default loadtester --events $TotalEvents --matches $UniqueMatches --bootstrap-servers redpanda:9092 --topic validated-events | Out-Host
    
    # 4. Start Engine and Timer
    Write-Host "[4/5] Starting $Engine..." -ForegroundColor Yellow
    $StartTime = [DateTime]::UtcNow
    
    if ($Engine -eq "Orleans") {
        docker compose -f deploy/docker-compose.yml --profile orleans up -d --build classifier-orleans | Out-Null
    }
    else {
        Write-Host "      Submitting Flink Job..." -ForegroundColor Yellow
        docker compose -f deploy/docker-compose.yml exec -T jobmanager ./bin/flink run -d /opt/flink/sql/java/target/first-match-classifier-1.0.0.jar | Out-Null
    }
    
    # Wait slightly to ensure consumer group is registered
    Start-Sleep -Seconds 5
    
    # 5. Monitor Lag
    Write-Host "[5/5] Monitoring Consumer Lag for group: $ConsumerGroup" -ForegroundColor Yellow
    $Lag = -1
    $PollsWithZeroLag = 0
    
    while ($true) {
        try {
            $output = docker compose -f deploy/docker-compose.yml exec -T redpanda rpk group describe $ConsumerGroup 2>&1
            # rpk prints the summary as "TOTAL-LAG" (v24+) or "TOTAL LAG" depending on version.
            $lagLine = $output | Select-String "TOTAL[- ]LAG"

            if ($lagLine -match "TOTAL[- ]LAG\s+(\d+)") {
                $Lag = [int]$matches[1]
                Write-Host "      Current Lag: $Lag"
                
                if ($Lag -eq 0) {
                    $PollsWithZeroLag++
                    if ($PollsWithZeroLag -ge 2) {
                        # Require 2 consecutive 0-lag polls to ensure it's fully settled
                        break
                    }
                }
                else {
                    $PollsWithZeroLag = 0
                }
            }
            else {
                Write-Host "      Waiting for consumer group to be registered..."
            }
        }
        catch {
            Write-Host "      Waiting for consumer group..."
        }
        
        Start-Sleep -Seconds 2
    }
    
    $EndTime = [DateTime]::UtcNow
    $TotalSeconds = ($EndTime - $StartTime).TotalSeconds
    $EventsPerSecond = $TotalEvents / $TotalSeconds
    
    Write-Host "`n==> $Engine finished processing in $($TotalSeconds.ToString('F2')) seconds!" -ForegroundColor Green
    Write-Host "==> Throughput: $($EventsPerSecond.ToString('N0')) events/sec" -ForegroundColor Green
    
    return $EventsPerSecond
}

# Always (re)build the shaded Flink application jar so we never submit a stale/thin jar.
# Check $LASTEXITCODE explicitly: on Windows PowerShell 5.1 a failing native command does not throw.
Write-Host "Building Flink application jar with Maven..." -ForegroundColor Yellow
mvn -f flink/java/pom.xml package -DskipTests | Out-Null
if ($LASTEXITCODE -ne 0 -or -Not (Test-Path "flink/java/target/first-match-classifier-1.0.0.jar")) {
    Write-Host "Maven build failed! Ensure Maven is installed and the Flink Java job compiles (mvn -f flink/java/pom.xml package)." -ForegroundColor Red
    exit 1
}

$OrleansTPS = Run-Benchmark -Engine "Orleans" -ConsumerGroup "orleans-classifier-group"
$FlinkTPS = Run-Benchmark -Engine "Flink" -ConsumerGroup "flink-classifier-java"

Write-Host "`n==================================================" -ForegroundColor Cyan
Write-Host " FINAL BENCHMARK RESULTS" -ForegroundColor Cyan
Write-Host "==================================================" -ForegroundColor Cyan
Write-Host "Events Injected: $TotalEvents"
Write-Host "Orleans Throughput : $($OrleansTPS.ToString('N0')) events/sec"
Write-Host "Flink Throughput   : $($FlinkTPS.ToString('N0')) events/sec"

if ($OrleansTPS -gt $FlinkTPS) {
    Write-Host "`nOrleans won by $(($OrleansTPS / $FlinkTPS).ToString('F2'))x !!" -ForegroundColor Green
}
else {
    Write-Host "`nFlink won by $(($FlinkTPS / $OrleansTPS).ToString('F2'))x !!" -ForegroundColor Green
}
