using EzRabbitMQ.Exceptions;
using EzRabbitMQ.Resiliency;
using RabbitMQ.Client;

namespace EzRabbitMQ;

/// <inheritdoc />
public class ConnectionService : IConnectionService
{
    private readonly ILogger<ConnectionService> _logger;
    private readonly IConnectionFactory _connectionFactory;
    private readonly ITelemetryService _telemetry;
    private readonly IPollyService _pollyService;

    /// <summary>
    /// Connection Service it will create a connection using the connection factory using polly service
    /// </summary>
    /// <param name="logger">Logger used for debug info.</param>
    /// <param name="connectionFactory">RabbitMQ connection factory.</param>
    /// <param name="telemetry">Telemetry service used to send trace on connection.</param>
    /// <param name="pollyService">Polly service is used to retry several times before fail.</param>
    public ConnectionService(
        ILogger<ConnectionService> logger,
        IConnectionFactory connectionFactory,
        ITelemetryService telemetry,
        IPollyService pollyService
    )
    {
        _logger = logger;
        _connectionFactory = connectionFactory;
        _telemetry = telemetry;
        _pollyService = pollyService;

        TryConnect();
    }

    /// <inheritdoc />
    public IConnection? Connection { get; private set; }

    /// <inheritdoc />
    public void TryConnect()
    {
        if (!Connection?.IsOpen ?? true)
        {
            _pollyService.TryExecute<CreateConnectionException>(() =>
                Connection = _connectionFactory.CreateConnectionAsync().ConfigureAwait(false).GetAwaiter().GetResult()
            );
            _telemetry.Trace("Connection established");
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        Connection?.Dispose();
        _logger.LogDebug("Connection disposed");
    }
}