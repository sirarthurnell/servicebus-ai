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

        _logger.LogInformation("Received {OrderId} from {Customer} (delivery #{Count})",
                evt.OrderId, evt.CustomerName, args.Message.DeliveryCount);

        // SIDE EFFECT: must happen exactly once per order.
        if (_processedOrders.Contains(evt.OrderId))
        {
            _logger.LogWarning("Duplicate {OrderId} from {Customer} (delivery #{Count})",
                evt.OrderId, evt.CustomerName, args.Message.DeliveryCount);
            await args.CompleteMessageAsync(args.Message);
            return;
        }
        _logger.LogWarning("CHARGED {Customer} for order {OrderId}", evt.CustomerName, evt.OrderId);
        _processedOrders.Add(evt.OrderId);

        // TEMP EXPERIMENT (revert after): crash once, AFTER the side effect, BEFORE Complete,
        // to force a redelivery and expose the duplicate.
        if (args.Message.DeliveryCount == 1 && evt.CustomerName == "Globex")
            throw new InvalidOperationException("Simulated crash after side effect");

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