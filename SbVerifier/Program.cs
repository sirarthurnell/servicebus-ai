using Azure.Messaging.ServiceBus;
using Contracts;

const string connectionString =
    "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;";

const string topic = "events";
const string subscription = "summarizer";

await using var client = new ServiceBusClient(connectionString);

// Point the receiver at the subscription's dead-letter subqueue
await using var receiver = client.CreateReceiver(topic, subscription, new ServiceBusReceiverOptions
{
    SubQueue = SubQueue.DeadLetter,
    ReceiveMode = ServiceBusReceiveMode.PeekLock
});

var dead = await receiver.ReceiveMessagesAsync(maxMessages: 10, maxWaitTime: TimeSpan.FromSeconds(5));
Console.WriteLine($"Dead-lettered messages: {dead.Count}\n");

foreach (var msg in dead)
{
    var evt = msg.Body.ToObjectFromJson<OrderEvent>();
    Console.WriteLine($"- {evt.CustomerName} [{evt.OrderId}]");
    Console.WriteLine($"  DeliveryCount : {msg.DeliveryCount}");
    Console.WriteLine($"  Reason        : {msg.DeadLetterReason}");
    Console.WriteLine($"  Description   : {msg.DeadLetterErrorDescription}");
    Console.WriteLine();
}