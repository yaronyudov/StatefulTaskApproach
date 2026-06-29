using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using SportsPipeline.Contracts;
using SportsPipeline.Domain;
using System.Collections.Generic;
using System.Text.Json;

namespace SportsPipeline.LoadTester;

public class Program
{
    public static async Task Main(string[] args)
    {
        // Simple argument parsing
        int totalEvents = 100000;
        int uniqueMatches = 5000;
        string bootstrapServers = "localhost:9092";
        string topic = "validated-events";
        
        for (int i = 0; i < args.Length; i++)
        {
            if (args[i] == "--events" && i + 1 < args.Length) int.TryParse(args[++i], out totalEvents);
            if (args[i] == "--matches" && i + 1 < args.Length) int.TryParse(args[++i], out uniqueMatches);
            if (args[i] == "--bootstrap-servers" && i + 1 < args.Length) bootstrapServers = args[++i];
            if (args[i] == "--topic" && i + 1 < args.Length) topic = args[++i];
        }

        Console.WriteLine($"Starting E2E Load Tester...");
        Console.WriteLine($"Target: {bootstrapServers} / Topic: {topic}");
        Console.WriteLine($"Total Events: {totalEvents:N0} across {uniqueMatches:N0} unique matches.");

        var baseTime = DateTimeOffset.UtcNow;
        var matchKeys = new List<string>(uniqueMatches);
        
        for (int i = 0; i < uniqueMatches; i++)
        {
            matchKeys.Add(MatchIdentity.ComputeKey(
                "Football",
                "Champions League",
                $"TeamA_{i}",
                $"TeamB_{i}"
            ));
        }

        var config = new ProducerConfig
        {
            BootstrapServers = bootstrapServers,
            LingerMs = 5,
            BatchNumMessages = 10000,
            Acks = Acks.Leader
        };

        using var producer = new ProducerBuilder<string, byte[]>(config).Build();

        Console.WriteLine("Generating and injecting events...");
        var sw = Stopwatch.StartNew();

        int successCount = 0;
        int errorCount = 0;
        
        int maxConcurrency = Environment.ProcessorCount * 2;
        using var semaphore = new SemaphoreSlim(maxConcurrency);
        var tasks = new List<Task>();

        for (int i = 0; i < totalEvents; i++)
        {
            await semaphore.WaitAsync();

            int matchIndex = i % uniqueMatches;
            var key = matchKeys[matchIndex];
            
            var rawState = new 
            {
                HomeScore = i % 5,
                AwayScore = i % 3,
                Period = "2H",
                ClockSeconds = i % 5400,
                HomeYellowCards = 0,
                AwayYellowCards = 0,
                HomeRedCards = 0,
                IsHomePossession = true,
                IsBallInPlay = true
            };

            var sportEvent = new SportEvent
            {
                SportType = "Football",
                CompetitionType = "Champions League",
                HomeTeam = new Team($"team-a-{matchIndex}", $"TeamA_{matchIndex}"),
                AwayTeam = new Team($"team-b-{matchIndex}", $"TeamB_{matchIndex}"),
                StartTime = baseTime,
                EventTime = DateTimeOffset.UtcNow,
                Metadata = new Dictionary<string, string>
                {
                    { Constants.EventContextKeys.RawJson, JsonSerializer.Serialize(rawState) },
                    { "providerId", "load-tester" }
                }
            };

            var payload = SportEventJson.Serialize(sportEvent);
            var message = new Message<string, byte[]> { Key = key, Value = payload };

            var task = producer.ProduceAsync(topic, message).ContinueWith(t => 
            {
                if (t.IsFaulted) Interlocked.Increment(ref errorCount);
                else Interlocked.Increment(ref successCount);
                
                semaphore.Release();
            });

            tasks.Add(task);

            if (i % 10000 == 0 && i > 0)
            {
                Console.WriteLine($"Injected {i:N0} events...");
            }
        }

        await Task.WhenAll(tasks);
        producer.Flush(TimeSpan.FromSeconds(10));
        
        sw.Stop();

        double eventsPerSecond = totalEvents / sw.Elapsed.TotalSeconds;

        Console.WriteLine("\n--- LOAD TEST COMPLETE ---");
        Console.WriteLine($"Total Time: {sw.Elapsed.TotalSeconds:N2} seconds");
        Console.WriteLine($"Successfully Injected: {successCount:N0}");
        Console.WriteLine($"Errors: {errorCount:N0}");
        Console.WriteLine($"Injection Throughput: {eventsPerSecond:N0} events/sec");
        Console.WriteLine("\nTo measure processing throughput:");
        Console.WriteLine("1. Check docker logs of Flink or Orleans to see when they finish processing.");
        Console.WriteLine("2. Check MongoDB collection size.");
    }
}
