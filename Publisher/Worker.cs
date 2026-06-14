using Azure.Messaging.ServiceBus;
using Contracts;

namespace Publisher;

public class Worker : BackgroundService
{
    private readonly ServiceBusClient _client;
    private readonly IConfiguration _config;
    private readonly ILogger<Worker> _logger;
    private readonly IHostApplicationLifetime _lifetime;

    public Worker(
        ServiceBusClient client,
        IConfiguration config,
        ILogger<Worker> logger,
        IHostApplicationLifetime lifetime)
    {
        _client = client;
        _config = config;
        _logger = logger;
        _lifetime = lifetime;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var topic = _config["ServiceBus:TopicName"]!;
        await using var sender = _client.CreateSender(topic);

        string[] customers = ["Acme Corp", "Globex", "Initech", "Umbrella", "Hooli"];
        string[] issues =
        [
            "Payment failed during checkout",
            "Cannot reset account password",
            "Duplicate charge on invoice",
            "Delivery address not updating",
            "App crashes on report export"
        ];

        for (int i = 0; i < 5; i++)
        {
            var evt = new OrderEvent(
                OrderId: Guid.NewGuid(),
                CustomerName: customers[i],
                Description: issues[i],
                OccurredAt: DateTimeOffset.UtcNow);

            var message = new ServiceBusMessage(BinaryData.FromObjectAsJson(evt))
            {
                ContentType = "application/json",
                MessageId = evt.OrderId.ToString()
            };

            await sender.SendMessageAsync(message, stoppingToken);
            _logger.LogInformation("Published OrderEvent {OrderId} for {Customer}", evt.OrderId, evt.CustomerName);
        }

        _logger.LogInformation("All events published. Shutting down.");
        _lifetime.StopApplication();
    }
}