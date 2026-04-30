using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace EzRabbitMQ.Messages;

/// <summary>
/// Message contains metadata information and data of type T
/// </summary>
/// <typeparam name="T">Type of the data contains in the message, accessible using the Data field</typeparam>
public class Message<T> : IMessage<T>
{
    /// <summary>
    /// Create a message from a rabbitMQ event and set data value
    /// </summary>
    /// <param name="event">RabbitMQ event arguments</param>
    /// <param name="data">Data you want to inject into the message</param>
    public Message(BasicDeliverEventArgs @event, T data)
    {
        Data = data;
        Exchange = @event.Exchange;
        RoutingKey = @event.RoutingKey;
        ConsumerTag = @event.ConsumerTag;
        DeliveryTag = @event.DeliveryTag;
        Redelivered = @event.Redelivered;
        BasicProperties = @event.BasicProperties;
    }

    /// <inheritdoc />
    public T Data { get; init; }

    /// <inheritdoc />
    public string Exchange { get; init; }

    /// <inheritdoc />
    public bool Redelivered { get; init; }

    /// <inheritdoc />
    public IReadOnlyBasicProperties BasicProperties { get; init; }

    /// <inheritdoc />
    public string ConsumerTag { get; init; }

    /// <inheritdoc />
    public ulong DeliveryTag { get; init; }

    /// <inheritdoc />
    public string RoutingKey { get; init; }
}