using Azure.Messaging.ServiceBus;
using Subscriber;
using Microsoft.Extensions.AI;
using OllamaSharp;

var builder = Host.CreateApplicationBuilder(args);

var connectionString = builder.Configuration.GetConnectionString("ServiceBus")
    ?? throw new InvalidOperationException("Missing ServiceBus connection string.");

builder.Services.AddSingleton(_ => new ServiceBusClient(connectionString));

var ollamaEndpoint = builder.Configuration["Ollama:Endpoint"]!;
var ollamaModel = builder.Configuration["Ollama:Model"]!;
builder.Services.AddSingleton<IChatClient>(_ => new OllamaApiClient(new Uri(ollamaEndpoint), ollamaModel));

builder.Services.AddHostedService<Worker>();

var host = builder.Build();
host.Run();