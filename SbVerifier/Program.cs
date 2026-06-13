using Azure.Messaging.ServiceBus;

// Emulator connection string. NOT a secret: UseDevelopmentEmulator=true tells the SDK
// to use the well-known local dev key, so "SAS_KEY_VALUE" is literal. Safe to commit.
const string connectionString =
    "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;";

const string topic = "events";
string[] subscriptions = ["summarizer", "audit"];

await using var client = new ServiceBusClient(connectionString);

// --- Publish 3 messages to the topic ---
await using var sender = client.CreateSender(topic);
for (int i = 1; i <= 3; i++)
{
    var id = Guid.NewGuid().ToString("N")[..8];
    await sender.SendMessageAsync(new ServiceBusMessage($"event #{i} [{id}]"));
    Console.WriteLine($"PUBLISHED to topic '{topic}': event #{i} [{id}]");
}

Console.WriteLine();

// --- Drain each subscription independently ---
foreach (var sub in subscriptions)
{
    await using var receiver = client.CreateReceiver(topic, sub,
        new ServiceBusReceiverOptions { ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete });

    var messages = await receiver.ReceiveMessagesAsync(maxMessages: 10, maxWaitTime: TimeSpan.FromSeconds(5));
    Console.WriteLine($"SUBSCRIPTION '{sub}' received {messages.Count} message(s):");
    foreach (var msg in messages)
        Console.WriteLine($"   - {msg.Body}");
    Console.WriteLine();
}