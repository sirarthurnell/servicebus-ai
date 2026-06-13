using Azure.Messaging.ServiceBus;

// Emulator connection string. NOT a secret: UseDevelopmentEmulator=true tells the SDK
// to use the well-known local dev key, so "SAS_KEY_VALUE" is literal. Safe to commit.
const string connectionString =
    "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;";

const string topic = "events";
// string[] subscriptions = ["summarizer", "audit"];
string[] subscriptions = ["summarizer"];

await using var client = new ServiceBusClient(connectionString);

// --- Publish messages to the topic ---
await using var sender = client.CreateSender(topic);
for (int i = 1; i <= 10; i++)
{
    var id = Guid.NewGuid().ToString("N")[..8];
    await sender.SendMessageAsync(new ServiceBusMessage($"event #{i} [{id}]"));
    Console.WriteLine($"PUBLISHED to topic '{topic}': event #{i} [{id}]");
}

Console.WriteLine();


var drain1 = CreateDrainSubscriptionTask("drainer1", topic, subscriptions, client);
var drain2 = CreateDrainSubscriptionTask("drainer2", topic, subscriptions, client);
await Task.WhenAll(drain1, drain2);

static async Task CreateDrainSubscriptionTask(string subscriber, string topic, string[] subscriptions, ServiceBusClient client)
{
    // --- Drain each subscription independently ---
    foreach (var sub in subscriptions)
    {
        await using var receiver = client.CreateReceiver(topic, sub,
            new ServiceBusReceiverOptions { ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete });

        while (true)
        {
            var messages = await receiver.ReceiveMessagesAsync(maxMessages: 1, maxWaitTime: TimeSpan.FromSeconds(2));
            if (messages.Count == 0) break;

            Console.WriteLine($"[{subscriber}]: SUBSCRIPTION '{sub}' received {messages.Count} message(s):");
            foreach (var msg in messages)
                Console.WriteLine($"   - {msg.Body}");
            Console.WriteLine();
        }
    }
}