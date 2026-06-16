using Azure.Messaging.ServiceBus;
using Microsoft.Agents.AI;
using Contracts;
using Microsoft.Extensions.AI;

namespace Subscriber;

public class Worker : BackgroundService
{
    private readonly ServiceBusClient _client;
    private readonly IConfiguration _config;
    private readonly ILogger<Worker> _logger;
    private ServiceBusProcessor? _processor;
    private readonly IChatClient _chatClient;
    private readonly AIAgent _agent;
    private readonly HashSet<Guid> _ticketedOrders = new();

    private static readonly Dictionary<string, string> Tiers = new(StringComparer.OrdinalIgnoreCase)
    {
        ["Acme Corp"] = "Premium",
        ["Globex"] = "Standard",
        ["Initech"] = "Standard",
        ["Umbrella"] = "Premium",
        ["Hooli"] = "Standard",
    };

    public Worker(ServiceBusClient client, IConfiguration config, ILogger<Worker> logger, IChatClient chatClient)
    {
        _client = client;
        _config = config;
        _logger = logger;
        _chatClient = chatClient;
        _agent = _chatClient.AsAIAgent(
        instructions: """
            You are a customer-support triage agent.
            First, call get_customer_tier with the customer's name to find their tier.
            Then reply with exactly one line: <Category> | <Urgency: Low/Medium/High> | <Tier>
            Bump urgency up one level for Premium customers.
            """,
        tools: [AIFunctionFactory.Create(
            GetCustomerTier,
            name: "get_customer_tier",
            description: "Looks up the support tier (Premium or Standard) of a customer by name.")]);
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

        // AI (Phase B): the agent triages the issue, calling tools as it sees fit.
        var prompt = $"Customer: {evt.CustomerName}\nIssue: {evt.Description}";
        var triage = await _agent.RunAsync(prompt, cancellationToken: args.CancellationToken);
        _logger.LogInformation("Triaged {OrderId} -> {Result}", evt.OrderId, triage.Text?.Trim());

        // Idempotent side effect: create a ticket once per order, keyed on the OrderId we hold HERE
        // (never routed through the LLM).
        if (_ticketedOrders.Add(evt.OrderId))
            _logger.LogWarning("TICKET created for {OrderId}: {Result}", evt.OrderId, triage.Text?.Trim());
        else
            _logger.LogInformation("Ticket for {OrderId} already exists; skipping duplicate", evt.OrderId);

        await args.CompleteMessageAsync(args.Message);
        _logger.LogInformation("Completed {OrderId}", evt.OrderId);
    }

    private string GetCustomerTier(string customerName)
    {
        var tier = Tiers.GetValueOrDefault(customerName, "Standard");
        _logger.LogInformation("[tool] get_customer_tier({Customer}) -> {Tier}", customerName, tier);
        return tier;
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