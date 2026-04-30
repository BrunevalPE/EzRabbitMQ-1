using EzRabbitMQ.DeletableResource;

namespace EzRabbitMQ;

/// <summary>
/// Mailbox exchange contains the current state of the exchange liked the the mailbox
/// </summary>
public sealed class MailboxExchange : DeletableResourceBase
{
    private readonly ISessionService _session;
    private readonly IMailboxOptions _options;
    private readonly ConsumerOptions _consumerOptions;

    /// <summary>
    /// Mailbox's exchange will declare and delete queue depending on mailbox options
    /// and consumer options.
    /// </summary>
    /// <param name="logger">Logger used to send information.</param>
    /// <param name="session">Mailbox's current session service.</param>
    /// <param name="options">Mailbox's current options.</param>
    /// <param name="consumerOptions">Mailbox's current consumer options.</param>
    public MailboxExchange(ILogger logger, ISessionService session, IMailboxOptions options, ConsumerOptions consumerOptions)
        : base(logger, session, consumerOptions.ExchangeRecreateMode)
    {
        _session = session;
        _options = options;
        _consumerOptions = consumerOptions;

        DeleteResourceIfNeeded();
        CreateResource();
    }

    private void DeclareExchange()
    {
        if (_options.ExchangeType is ExchangeType.RpcClient or ExchangeType.RpcServer) return;

        using var operation = _session.Telemetry.Request(_options, "Mailbox declare exchange");

        try
        {
            _session.Model?.ExchangeDeclareAsync(_options.ExchangeName, _options.ExchangeType.Type(),
                _consumerOptions.ExchangeDurable,
                _consumerOptions.ExchangeAutoDelete, _consumerOptions.ExchangeDeclareArguments).GetAwaiter().GetResult();
        }
        catch (Exception e)
        {
            HandleException(e);
        }
    }

    /// <inheritdoc />
    protected override void DeleteResource(bool ifUnused, bool _)
    {
        _session.Model?.ExchangeDeleteAsync(_options.ExchangeName, ifUnused).GetAwaiter().GetResult();
    }

    /// <inheritdoc />
    protected override void CreateResource()
    {
        DeclareExchange();
    }
}