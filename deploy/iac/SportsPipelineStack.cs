using Amazon.CDK;
using Amazon.CDK.AWS.EC2;
using Amazon.CDK.AWS.ECS;
using Amazon.CDK.AWS.DynamoDB;
using Amazon.CDK.AWS.OpenSearchService;
using Constructs;

namespace SportsPipeline.Infra;

/// <summary>
/// SKELETON infrastructure for the sports pipeline. It names the AWS services and how they wire
/// together; props are intentionally minimal (sizing, security groups, IAM, and the MSK / Managed
/// Flink / MongoDB Atlas L1-or-L2 details are TODOs). It is excluded from the solution build and is
/// meant to be read alongside docs/architecture.md, not deployed as-is.
/// </summary>
public sealed class SportsPipelineStack : Stack
{
    internal SportsPipelineStack(Construct scope, string id, IStackProps props) : base(scope, id, props)
    {
        var vpc = new Vpc(this, "Vpc", new VpcProps { MaxAzs = 2 });

        // Mapping database (provider -> domain translation rules), keyed by providerId.
        _ = new Table(this, "MappingTable", new TableProps
        {
            PartitionKey = new Amazon.CDK.AWS.DynamoDB.Attribute { Name = "providerId", Type = AttributeType.STRING },
            BillingMode = BillingMode.PAY_PER_REQUEST,
        });

        // Discovery index: Flink sinks first-match rows here; the Query API searches it.
        _ = new Domain(this, "Discovery", new DomainProps
        {
            Version = EngineVersion.OPENSEARCH_2_13,
            Vpc = vpc,
            Capacity = new CapacityConfig { DataNodes = 2 },
        });

        // Details / system of record: MongoDB Atlas on AWS (real MongoDB — HA replica sets, read
        // replicas, and sharding if writes outgrow one primary). Atlas is not a native CloudFormation
        // resource: provision it with the MongoDB Atlas CDK L1 resources (the `awscdk-resources-mongodbatlas`
        // package / Atlas CloudFormation registry) or Terraform, and connect it to this VPC via
        // AWS PrivateLink. Flink writes ONE final-state doc per window (_id=matchId); the Query API does
        // point lookups by id. Connection: Atlas SRV uri, tls=true, retryWrites=false, secret in Secrets Manager.
        // e.g. new AtlasBasic(this, "Details", new AtlasBasicProps { ... ClusterName="sports", Region="US_EAST_1" });

        // ECS cluster hosting the C# services (one scrapper service per provider, sse, query-api).
        var cluster = new Cluster(this, "Cluster", new ClusterProps { Vpc = vpc });
        AddFargateService(cluster, "ScrapperProviderA");  // one per provider (blast-radius isolation)
        AddFargateService(cluster, "Sse");
        AddFargateService(cluster, "QueryApi");

        // TODO (skeleton): the following are declared in docs/architecture.md and added here as
        // CfnResource / dedicated constructs when deploying:
        //   * Amazon MSK (Kafka)  -> CfnCluster
        //   * Amazon Managed Service for Apache Flink (runs match_pipeline.sql / the Java jar)
        //   * Secrets Manager (provider credentials referenced by ProviderConfig.AuthSecretName)
        //   * optional ElastiCache Redis (SSE live snapshot/TTL backplane)
        //   * ALB fronting Sse + QueryApi
    }

    private void AddFargateService(Cluster cluster, string name)
    {
        var taskDef = new FargateTaskDefinition(this, $"{name}Task");
        taskDef.AddContainer($"{name}Container", new ContainerDefinitionOptions
        {
            Image = ContainerImage.FromRegistry("public.ecr.aws/amazonlinux/amazonlinux:latest"), // placeholder
        });
        _ = new FargateService(this, $"{name}Service", new FargateServiceProps
        {
            Cluster = cluster,
            TaskDefinition = taskDef,
            DesiredCount = 1,
        });
    }
}
