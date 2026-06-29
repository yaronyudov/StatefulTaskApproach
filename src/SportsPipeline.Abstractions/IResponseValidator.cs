namespace SportsPipeline.Abstractions;

public interface IResponseValidator
{
    void ValidateTransport(HttpResponseMessage response, long? contentLength);
    void ValidatePayloadSize(int eventCount);
}
