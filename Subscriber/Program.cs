using Azure.Messaging.ServiceBus;
using Subscriber;

var builder = Host.CreateApplicationBuilder(args);

var connectionString = builder.Configuration.GetConnectionString("ServiceBus")
    ?? throw new InvalidOperationException("Missing ServiceBus connection string.");

builder.Services.AddSingleton(_ => new ServiceBusClient(connectionString));
builder.Services.AddHostedService<Worker>();

var host = builder.Build();
host.Run();