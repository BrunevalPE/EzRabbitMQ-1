using EzRabbitMQ.Exceptions;
using EzRabbitMQ.Queue;
using RabbitMQ.Client;

namespace EzRabbitMQ;

/// <summary>
/// Mailbox Base
/// </summary>
public abstract class MailboxBase : ConsumerHandleBase
{
    /// <summary>
    /// Mailbox queue handle creation, configuration and deletion of the queue
    /// </summary>
    protected readonly MailboxQueue Queue;

    /// <summary>
    /// Mailbox exchange handle creation, configuration and deletion of the exchange
    /// </summary>
    protected readonly MailboxExchange Exchange;

    /// <inheritdoc />
    protected MailboxBase(
        ILogger logger,
        IMailboxOptions options,
        ISessionService session,
        ConsumerOptions consumerOptions
    ) : base(logger, options, session, consumerOptions)
    {
        ConfigureHeaders();

        try
        {
            Exchange = new MailboxExchange(logger, session, options, consumerOptions);
            Queue = new MailboxQueue(logger, session, options, consumerOptions);
            Consume();
        }
        catch
        {
            Dispose();
            throw;
        }
    }

    private void ConfigureHeaders()
    {
        if (Session.Properties is null)
        {
            Logger.LogError("Unable to configure channel properties");
            return;
        }

        Session.Properties.Headers ??= new Dictionary<string, object>();

        foreach (var (key, value) in Options.SessionHeaders)
        {
            Session.Properties.Headers.TryAdd(key, value);
        }

        Session.Properties.CorrelationId = Options.CorrelationId;
    }

    private void Consume()
    {
        using var operation = Session.Telemetry.Request(Options, $"{nameof(MailboxBase)} Consumer bound");

        try
        {
            var consumerTag =
                Session.Model.BasicConsumeAsync(Options.QueueName, ConsumerOptions.AutoAck, ConsumerTag, false, false, null, Consumer).ConfigureAwait(false).GetAwaiter().GetResult();
            Logger.LogDebug("Consumer connected to queue : {QueueName} tag: {ConsumerTag}", Options.QueueName,
                consumerTag);
        }
        catch (Exception e)
        {
            throw new ConfigureMailboxException(e);
        }

        ConfigurePrefetch();
    }

    private void ConfigurePrefetch()
    {
        if (Options.ExchangeType is ExchangeType.RpcServer)
        {
            Session.Model?.BasicQosAsync(0, 1, false).ConfigureAwait(false).GetAwaiter().GetResult();
            return;
        }

        if (ConsumerOptions.PrefetchSize > 0 || ConsumerOptions.PrefetchCount > 0)
        {
            Session.Model?.BasicQosAsync(ConsumerOptions.PrefetchSize, ConsumerOptions.PrefetchCount,
                ConsumerOptions.PrefetchGlobal).ConfigureAwait(false).GetAwaiter().GetResult();
        }
    }
}