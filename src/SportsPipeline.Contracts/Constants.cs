namespace SportsPipeline.Contracts;

public static class Constants
{
    public static class KafkaHeaders
    {
        public const string ProviderId = "ProviderId";
    }

    public static class EventContextKeys
    {
        public const string ProviderId = "providerId";
        public const string RawJson = "rawJson";
    }
}
