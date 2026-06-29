using SportsPipeline.Domain;
using SportsPipeline.Scraper;
using Microsoft.Extensions.Options;
using Xunit;

namespace SportsPipeline.Tests;

public class ResponseValidatorTests
{
    private static ResponseValidator Validator(ValidationOptions? options = null)
    {
        var scraperOptions = new ScraperOptions();
        if (options != null) scraperOptions.Provider.Validation = options;
        return new ResponseValidator(Options.Create(scraperOptions));
    }


    [Fact]
    public void Rejects_event_count_over_limit()
    {
        var options = new ValidationOptions { MaxEventsPerResponse = 2 };
        Assert.Throws<ValidationException>(() => Validator(options).ValidatePayloadSize(3));
    }
}
