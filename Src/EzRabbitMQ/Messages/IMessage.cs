using RabbitMQ.Client;

namespace EzRabbitMQ.Messages;

/// <summary>
/// Message wrapper containing data and metadata
/// </summary>
/// <typeparam name="T">Type of the data</typeparam>
public interface IMessage<T>
{
    /// <summary>
    /// Data
    /// </summary>
    public T Data { get; init; }

    /// <summary>
    /// Message's exchange
    /// </summary>
    public string Exchange { get; init; }

    /// <summary>
    /// Message
    /// </summary>
    public string RoutingKey { get; init; }

    /// <summary>
    /// Message consumer tag
    /// </summary>
    public string ConsumerTag { get; init; }

    /// <summary>
    /// Message delivery tag
    /// </summary>
    public ulong DeliveryTag { get; init; }

    /// <summary>
    /// Message is redelivered
    /// </summary>
    public bool Redelivered { get; init; }

    /// <summary>
    /// Message properties
    /// </summary>
    public IReadOnlyBasicProperties BasicProperties { get; init; }
}