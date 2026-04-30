using EzRabbitMQ.Exceptions;
using EzRabbitMQ.Extensions;
using EzRabbitMQ.Reflection;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace EzRabbitMQ;

/// <summary>
/// Application local state used to store global consumer indexes
/// </summary>
public static class AppState
{
    /// <summary>
    /// Store a counter for each assembly to scope consumer with readable name
    /// </summary>
    public static readonly Lazy<ConcurrentDictionary<string, int>> MailBoxIndexes = new(() => new ConcurrentDictionary<string, int>());
}

/// <summary>
/// Consumer handle basic session events
/// </summary>
public abstract class ConsumerHandleBase : IDisposable
{
    private const string OnMessageHandleName = nameof(IMailboxHandler<object>.OnMessageHandle);
    private const string OnMessageHandleAsyncName = nameof(IMailboxHandlerAsync<object>.OnMessageHandleAsync);

    /// <summary>
    /// Consumer tag, representing the unique consumer identifier
    /// </summary>
    protected readonly string ConsumerTag = ConsumersManager.CreateTag();

    /// <summary>
    /// Logger
    /// </summary>
    protected readonly ILogger Logger;

    /// <summary>
    /// Mailbox options
    /// </summary>
    protected readonly IMailboxOptions Options;

    /// <summary>
    /// Current rabbitMQ session
    /// </summary>
    protected readonly ISessionService Session;

    /// <summary>
    /// Consumer options
    /// </summary>
    protected readonly ConsumerOptions ConsumerOptions;

    /// <summary>
    /// The service handle RabbitMQ events
    /// </summary>
    /// <param name="logger">ILogger</param>
    /// <param name="options">Mailbox options, some option will be used to create exchange/queue/bindings</param>
    /// <param name="session">Current RabbitMQ session</param>
    /// <param name="consumerOptions">Consumer options, these options are used to change the consumer behavior, see <see cref="ConsumerOptions"/></param>
    protected ConsumerHandleBase(
        ILogger logger,
        IMailboxOptions options,
        ISessionService session,
        ConsumerOptions consumerOptions
    )
    {
        Logger = logger;
        Session = session;
        Options = options;
        ConsumerOptions = consumerOptions;

        Session.Polly.TryExecute<CreateConsumerException>(CreateConsumer);
    }

    /// <summary>
    /// RabbitMQ Consumer
    /// </summary>
    protected IAsyncBasicConsumer? Consumer { get; private set; }

    /// <inheritdoc />
    public void Dispose()
    {
        Session.Dispose();
    }

    private void CreateConsumer()
    {
        using var operation = Session.Telemetry.Request(Options, "BasicConsumer created");
        Consumer = CreateAsyncConsumer();
    }

    private IAsyncBasicConsumer CreateAsyncConsumer()
    {
        var consumer = new AsyncEventingBasicConsumer(Session.Model);
        consumer.ShutdownAsync += AsyncConsumer_ShutdownAsync;
        consumer.ReceivedAsync += AsyncConsumer_ReceivedAsync;
        consumer.RegisteredAsync += AsyncConsumer_RegisteredAsync;
        consumer.UnregisteredAsync += AsyncConsumer_UnregisteredAsync;
        return consumer;
    }

    private Task AsyncConsumer_RegisteredAsync(object? sender, ConsumerEventArgs @event)
    {
        Session.Telemetry.Trace("Consumer registered", new() {{"consumerTag", ConsumerTag}});

        return Task.CompletedTask;
    }

    private Task AsyncConsumer_ReceivedAsync(object? sender, BasicDeliverEventArgs @event)
    {
        using var operation = Session.Telemetry.Dependency(Options, "OnMessageHandle");

        operation.Telemetry.Context.Operation.SetTelemetry(@event.BasicProperties);

        return MessageHandle(sender, @event);
    }

    /// <summary>
    /// Event raised on message received by the consumer.
    /// </summary>
    /// <param name="sender">Event sender.</param>
    /// <param name="event">RabbitMQ raw event.</param>
    protected virtual async Task MessageHandle(object? sender, BasicDeliverEventArgs @event)
    {
        var messageTypeText = @event.BasicProperties.Type;

        var currentType = GetType();
        var method = CachedReflection.FindMethodToInvoke(currentType, messageTypeText, OnMessageHandleName);
        var asyncMethod = CachedReflection.FindMethodToInvoke(currentType, messageTypeText, OnMessageHandleAsyncName);
        if (method is null && asyncMethod is null)
        {
            Logger.LogError("Not found handle exception");
            throw new ReflectionNotFoundHandleException(GetType().Name, $"{OnMessageHandleName}|{OnMessageHandleAsyncName}", messageTypeText);
        }

        var message = @event.GetMessage(Session.Config);

        await TryExecuteOnMessage(@event, message, method, asyncMethod);
    }

    private async Task TryExecuteOnMessage(BasicDeliverEventArgs @event, object message, MethodBase? method, MethodInfo? asyncMethod)
    {
        try
        {
            if (asyncMethod is not null)
            {
                var task = asyncMethod.Invoke(this, new[] {message, Session.SessionToken});
                if (task is not null)
                {
                    await (dynamic) task;
                }
            }
            else
            {
                if (method is null)
                {
                    Logger.LogError("Unable to find any matching method called: {MethodName} in your implementation : {ImplementationName}", OnMessageHandleName, GetType().Name);
                    await HandleOnMessageExceptionAsync(@event);
                    return;
                }

                method.Invoke(this, new[] {message});
            }

            if (!ConsumerOptions.AutoAck)
            {
                await (Session.Model?.BasicAckAsync(@event.DeliveryTag, ConsumerOptions.AckMultiple) ?? ValueTask.CompletedTask);
            }
        }
        catch
        {
            await HandleOnMessageExceptionAsync(@event);
            throw;
        }
    }

    private async Task HandleOnMessageExceptionAsync(BasicDeliverEventArgs @event)
    {
        if (ConsumerOptions.AutoAck)
        {
            return;
        }

        await (Session.Model?.BasicRejectAsync(@event.DeliveryTag, false) ?? ValueTask.CompletedTask);
    }

    private Task AsyncConsumer_ShutdownAsync(object? sender, ShutdownEventArgs e)
    {
        if (e.ReplyCode != 200)
        {
            Logger.LogError("Consumer shutdown : {ReplyText}", e.ReplyText);

            CreateConsumer();

            return Task.CompletedTask;
        }

        Logger.LogDebug("Consumer shutdown {ConsumerTag}", ConsumerTag);

        Session.Telemetry.Trace("Consumer shutdown", new() {{"consumerTag", ConsumerTag}});

        return Task.CompletedTask;
    }

    private Task AsyncConsumer_UnregisteredAsync(object? sender, ConsumerEventArgs e)
    {
        Session.Telemetry.Trace("Consumer unregistered", new() {{"consumerTag", ConsumerTag}});

        Logger.LogDebug("Consumer unregistered : {ConsumerTag}", ConsumerTag);

        return Task.CompletedTask;
    }
}