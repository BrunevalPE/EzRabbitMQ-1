using EzRabbitMQ.Resiliency;
using RabbitMQ.Client;

namespace EzRabbitMQ;

/// <summary>
/// Handle the current rabbitMQ channel
/// </summary>
public interface ISessionService : IDisposable
{
    /// <summary>
    /// RabbitMQ channel
    /// </summary>
    IChannel? Model { get; }

    /// <summary>
    /// Session's properties
    /// </summary>
    BasicProperties? Properties { get; }

    /// <summary>
    /// Telemetry service
    /// </summary>
    ITelemetryService Telemetry { get; }

    /// <summary>
    /// Polly service
    /// </summary>
    IPollyService Polly { get; }

    /// <summary>
    /// EzRabbitMQ configuration
    /// </summary>
    EzRabbitMQConfig Config { get; }

    /// <summary>
    /// Override the current session properties
    /// </summary>
    /// <param name="properties">RabbitMQ session properties</param>
    void SetProperties(BasicProperties properties);

    /// <summary>
    /// Check the current channel and if isClosed is true the current channel
    /// will be disposed and a new channel will be created
    /// </summary>
    void RefreshChannelIfClosed();

    /// <summary>
    /// Session Cancellation Token, cancel on disconnection
    /// </summary>
    CancellationToken SessionToken { get; }
}