namespace Contracts;

/// <summary>
/// Domain event published when an order is registered.
/// Immutable: it describes something that already happened.
/// </summary>
public record OrderEvent(
    Guid OrderId,
    string CustomerName,
    string Description,
    DateTimeOffset OccurredAt);