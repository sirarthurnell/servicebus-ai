using Azure.Messaging.ServiceBus;
using Contracts;

namespace Subscriber;

public class Worker : BackgroundService
{
    private readonly ServiceBusClient _client;
    private readonly IConfiguration _config;
    private readonly ILogger<Worker> _logger;
    private ServiceBusProcessor? _processor;
    private readonly HashSet<Guid> _processedOrders = new();

    public Worker(ServiceBusClient client, IConfiguration config, ILogger<Worker> logger)
    {
        _client = client;
        _config = config;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var topic = _config["ServiceBus:TopicName"]!;
        var subscription = _config["ServiceBus:SubscriptionName"]!;

        _processor = _client.CreateProcessor(topic, subscription, new ServiceBusProcessorOptions
        {
            AutoCompleteMessages = false,   // peek-lock: we decide the message's fate
            MaxConcurrentCalls = 1
        });

        _processor.ProcessMessageAsync += OnMessageAsync;
        _processor.ProcessErrorAsync += OnErrorAsync;

        await _processor.StartProcessingAsync(stoppingToken);
        _logger.LogInformation("Listening on '{Topic}/{Subscription}'...", topic, subscription);
    }

    private async Task OnMessageAsync(ProcessMessageEventArgs args)
    {
        OrderEvent? evt;
        try
        {
            evt = args.Message.Body.ToObjectFromJson<OrderEvent>();
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Malformed message {MessageId}", args.Message.MessageId);
            evt = null;
        }

        if (evt is null)
        {
            await args.DeadLetterMessageAsync(args.Message,
                deadLetterReason: "DeserializationFailed",
                deadLetterErrorDescription: "Body is not a valid OrderEvent.");
            _logger.LogWarning("Dead-lettered {MessageId}", args.Message.MessageId);
            return;
        }

        _logger.LogInformation("Received {OrderId} from {Customer}: {Description}",
            evt.OrderId, evt.CustomerName, evt.Description);

        // Real processing goes here. On Day 4 this becomes the AI agent.

        await args.CompleteMessageAsync(args.Message);
        _logger.LogInformation("Completed {OrderId}", evt.OrderId);
    }

    private Task OnErrorAsync(ProcessErrorEventArgs args)
    {
        _logger.LogError(args.Exception, "Service Bus error ({Source})", args.ErrorSource);
        return Task.CompletedTask;
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_processor is not null)
        {
            await _processor.StopProcessingAsync(cancellationToken);
            await _processor.DisposeAsync();
        }
        await base.StopAsync(cancellationToken);
    }
}