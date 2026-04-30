using System.Text.Json;
using EzRabbitMQ.DeletableResource;
using EzRabbitMQ.Exceptions;

namespace EzRabbitMQ.Queue;

/// <summary>
/// Mailbox's queue will declare the queue, delete if needed and create the binding between the consumer and the exchange 
/// </summary>
public sealed class MailboxQueue : DeletableResourceBase
{
    private readonly ILogger _logger;
    private readonly ISessionService _session;
    private readonly IMailboxOptions _options;
    private readonly ConsumerOptions _consumerOptions;

    /// <summary>
    /// Mailbox's queue will declare the queue, delete if needed and create the binding between the consumer and the exchange
    /// </summary>
    /// <param name="logger">Logger to log debug information.</param>
    /// <param name="session">Current mailbox's information.</param>
    /// <param name="options">Current mailbox's options.</param>
    /// <param name="consumerOptions">Current mailbox's consumer options.</param>
    public MailboxQueue(ILogger logger, ISessionService session, IMailboxOptions options, ConsumerOptions consumerOptions)
        : base(logger, session, consumerOptions.QueueRecreateMode)
    {
        _logger = logger;
        _session = session;
        _options = options;
        _consumerOptions = consumerOptions;

        DeleteResourceIfNeeded();

        CreateResource();
    }

    private void QueueDeclare()
    {
        using var operation = _session.Telemetry.Request(_options, $"{nameof(MailboxBase)} Queue declared.");

        ConfigureQueueSizeLimit();

        ConfigureDeadLetter();

        ConfigureQueueMode();

        ConfigureMaxPriority();

        if (_options.ExchangeType is ExchangeType.RpcClient) return;

        var exclusive = _options.ExchangeType is ExchangeType.RpcServer || _consumerOptions.QueueExclusive;

        try
        {
            var response = _session.Model?.QueueDeclareAsync(_options.QueueName,
                _consumerOptions.QueueDurable,
                exclusive, _consumerOptions.QueueAutoDelete,
                _consumerOptions.QueueDeclareArguments).ConfigureAwait(false).GetAwaiter().GetResult();

            _logger.LogDebug("Queue declare response: {Response} for {Exchange}", JsonSerializer.Serialize(response), _options.ExchangeName);
        }
        catch (Exception e)
        {
            HandleException(e);
        }
    }

    /// <inheritdoc />
    protected override void DeleteResource(bool ifUnused, bool ifEmpty)
    {
        _session.Model?.QueueDeleteAsync(_options.QueueName, ifUnused, ifEmpty).ConfigureAwait(false).GetAwaiter().GetResult();
    }

    /// <inheritdoc />
    protected override void CreateResource()
    {
        QueueDeclare();
        QueueBind();
    }

    private void ConfigureQueueSizeLimit()
    {
        if (_consumerOptions.QueueSizeLimit > 0)
        {
            _consumerOptions.QueueDeclareArguments.TryAdd(Constants.QueueArgsMaxLength, _consumerOptions.QueueSizeLimit);
            _logger.LogDebug("Queue size limit has been set");
        }
    }

    private void ConfigureDeadLetter()
    {
        if (_consumerOptions.DeadLetterExchangeName is not null)
        {
            _consumerOptions.QueueDeclareArguments.TryAdd(Constants.QueueArgsDeadLetterExchange,
                _consumerOptions.DeadLetterExchangeName);
            _logger.LogDebug("Queue dead letter exchange name has been set");
        }

        if (_consumerOptions.DeadLetterRoutingKey is not null)
        {
            _consumerOptions.QueueDeclareArguments.TryAdd(Constants.QueueArgsDeadLetterRoutingKey,
                _consumerOptions.DeadLetterRoutingKey);
            _logger.LogDebug("Queue dead letter routing key has been set");
        }
    }

    private void ConfigureQueueMode()
    {
        if (_consumerOptions.QueueMode is not QueueMode.Default)
        {
            _consumerOptions.QueueDeclareArguments.TryAdd(Constants.QueueArgsMode, _consumerOptions.QueueMode.GetTextValue());
            _logger.LogDebug("Queue Mode has been set");
        }
    }

    private void ConfigureMaxPriority()
    {
        if (!_consumerOptions.QueueMaxPriority.HasValue) return;

        _consumerOptions.QueueBindArguments.TryAdd(Constants.QueueArgsMaxPriority, _consumerOptions.QueueMaxPriority.Value.ToString());

        _logger.LogDebug("Queue max priority has been set");
    }

    private void QueueBind()
    {
        if (_options.ExchangeType is ExchangeType.RpcClient) return;

        foreach (var (key, value) in _options.QueueBindingHeaders)
        {
            _consumerOptions.QueueBindArguments.TryAdd(key, value);
        }

        using var operation = _session.Telemetry.Request(_options, $"{nameof(MailboxBase)} Queue bound.");

        try
        {
            _session.Model?.QueueBindAsync(_options.QueueName, _options.ExchangeName, _options.RoutingKey,
                _consumerOptions.QueueBindArguments).ConfigureAwait(false).GetAwaiter().GetResult();
        }
        catch (Exception e)
        {
            HandleException(e);
            if (RabbitMQExceptionInterceptor.ParseRabbitMQException(e) is RabbitMQExceptionType.Unknown)
                throw;
        }
    }
}