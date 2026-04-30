using System.Runtime.Serialization;
using EzRabbitMQ.Exceptions;
using EzRabbitMQ.Extensions;
using RabbitMQ.Client;

namespace EzRabbitMQ;

/// <inheritdoc />
public class Producer : IDisposable
{
    private readonly ILogger<Producer> _logger;
    private readonly IProducerOptions _options;
    private readonly ISessionService _session;

    /// <summary>
    /// Producer can publish messages to rabbitMQ server
    /// </summary>
    /// <param name="logger">Logger used depending on LogLevel</param>
    /// <param name="options">Producer options see <see cref="IProducerOptions"/></param>
    /// <param name="session">Current session with rabbitMQ server</param>
    public Producer(
        ILogger<Producer> logger,
        IProducerOptions options,
        ISessionService session
    )
    {
        _logger = logger;
        _options = options;
        _session = session;

        SetPropertiesFromOptions();
    }

    /// <summary>
    /// Publish data pass as parameter 
    /// </summary>
    /// <param name="data">Data sending to the rabbitMQ</param>
    /// <typeparam name="T">Type of the data you are sending</typeparam>
    public void Publish<T>(T data)
    {
        using var operation = _session.Telemetry.Dependency(_options, "publish message");

        if (data is null)
        {
            _logger.LogWarning("Unable to publish null message");
            return;
        }

        _session.Properties?.SetTelemetry(operation.Telemetry.Context.Operation);

        try
        {
            var body = _session.Config.SerializeData(data);

            if (body is null)
            {
                throw new SerializationException("Error occured while serializing data");
            }

            _logger.LogDebug(
                "message sent to rabbit: exchange '{ExchangeName}' routingKey: '{RoutingKey}', size: {Bytes} bytes",
                _options.ExchangeName, _options.RoutingKey, body.Length);

            var dataType = data.GetType().AssemblyQualifiedName;

            if (dataType is null)
            {
                throw new InvalidProgramException("Unable to get data type");
            }

            _session.Polly.TryExecute<PublishException>(() => Publish(dataType, body));
        }
        catch (Exception e)
        {
            operation.Exception(e);
            throw;
        }
    }

    private void SetPropertiesFromOptions()
    {
        if (_session.Properties is null)
        {
            _logger.LogError("Unable to set session properties because channel properties field is null");
            return;
        }

        _session.Properties.DeliveryMode = _options.Properties.DeliveryMode == DeliveryMode.Persistent
            ? DeliveryModes.Persistent
            : DeliveryModes.Transient;

        if (_options.Properties.Expiration.HasValue)
        {
            _session.Properties.Expiration =
                _options.Properties.Expiration.Value.TotalMilliseconds.ToString(CultureInfo.InvariantCulture);
        }

        if (_options.Properties.Headers.Any())
        {
            foreach (var (key, value) in _options.Properties.Headers)
            {
                _session.Properties.Headers ??= new Dictionary<string, object>();

                _session.Properties.Headers.Add(key, value);
            }
        }

        if (_options.Properties.Priority.HasValue)
        {
            _session.Properties.Priority = _options.Properties.Priority.Value;
        }

        if (!string.IsNullOrWhiteSpace(_options.Properties.ReplyTo))
        {
            _session.Properties.CorrelationId = _options.Properties.CorrelationId;
            _session.Properties.ReplyTo = _options.Properties.ReplyTo;
        }
    }

    private void Publish(string type, byte[] body)
    {
        if (_session.Properties is null)
        {
            _logger.LogError("Unable to publish message without channel's session properties");
            return;
        }

        _session.Properties.Type = type;
        _session.Model.BasicPublishAsync(_options.ExchangeName, _options.RoutingKey, false, _session.Properties, new ReadOnlyMemory<byte>(body)).ConfigureAwait(false).GetAwaiter().GetResult();
    }

    /// <inheritdoc />
    public void Dispose()
    {
        _session.Dispose();
        _logger.LogDebug("Producer disposed");
    }
}