using System.Text.Json;
using EzRabbitMQ.Exceptions;
using EzRabbitMQ.Extensions;
using EzRabbitMQ.Reflection;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace EzRabbitMQ;

/// <summary>
/// Rpc client able to create request and receive response 
/// </summary>
public class RpcClient : MailboxBase
{
    private readonly ConcurrentDictionary<Type, BlockingCollection<object>> _listeners = new();

    /// <summary>
    /// RpcClient can send request and wait for response
    /// </summary>
    public RpcClient(
        ILogger<RpcClient> logger,
        RpcClientMailboxOptions options,
        ISessionService session,
        ConsumerOptions consumerOptions
    )
        : base(logger, options, session, consumerOptions)
    {
    }

    /// <summary>
    /// Send request to the server and wait for a response.
    /// </summary>
    /// <param name="request">Request for the rpc server</param>
    /// <param name="cancellationToken">Cancellation Token</param>
    /// <returns>Task of nullable</returns>
    public Task<T?> CallAsync<T>(object request, CancellationToken cancellationToken) where T : class
    {
        return Task.Run(() => Call<T>(request, cancellationToken), cancellationToken);
    }

    /// <summary>
    /// Send request to the server and wait for a response.
    /// </summary>
    /// <param name="request">Request object you want to send to the server</param>
    /// <param name="externalCancellationToken">Optional CancellationToken preventing from waiting until request timeout if cancellation is raised</param>
    /// <typeparam name="T">IRpcResponse type</typeparam>
    /// <returns>Nullable IRpcResponse</returns>
    /// <exception cref="RpcClientCastException">The response received doesnt match with the method argument T type</exception>
    public T? Call<T>(object request, CancellationToken externalCancellationToken = default) where T : class
    {
        if (Session.Config.RpcPollyPolicy is not null)
        {
            return Session.Config.RpcPollyPolicy.ExecuteAsync(async () => (await CallInternal<T>(request, externalCancellationToken))!).GetAwaiter().GetResult() as T;
        }

        return CallInternal<T>(request, externalCancellationToken).GetAwaiter().GetResult();
    }

    private Task<T?> CallInternal<T>(object request, CancellationToken externalCancellationToken = default) where T : class
    {
        using var op = Session.Telemetry.Dependency(Options,
            "RpcClient request");

        if (Session.Model is null) return Task.FromResult<T?>(default);

        var blockingColl = _listeners.GetOrAdd(typeof(T), new BlockingCollection<object>());

        var sw = Stopwatch.StartNew();

        var body = Session.Config.SerializeData(request);
        var props = new BasicProperties
        {
            ReplyTo = Constants.RpcReplyToQueue,
            CorrelationId = Options.CorrelationId,
            Type = request.GetType().AssemblyQualifiedName
        };

        Session.Model.BasicPublishAsync(ExchangeType.RpcServer.Name(), Options.RoutingKey, false, props, new ReadOnlyMemory<byte>(body)).GetAwaiter().GetResult();
        Logger.LogDebug("Message sent to {Exchange} routing Key: {RoutingKey}", ExchangeType.RpcServer.Name(), Options.RoutingKey);

        var timeoutCts = new CancellationTokenSource(ConsumerOptions.RpcCallTimeout);
        using var sharedCts = CancellationTokenSource.CreateLinkedTokenSource(timeoutCts.Token, externalCancellationToken);

        try
        {
            var response = blockingColl.Take(sharedCts.Token);

            sw.Stop();

            Logger.LogDebug("Call {CorrelationId} took {Elapsed}ms", Options.CorrelationId, sw.ElapsedMilliseconds.ToString());

            T typedResponse = (response as T) ?? throw new RpcClientCastException(typeof(T));

            return Task.FromResult<T?>(typedResponse);
        }
        catch (OperationCanceledException)
        {
            Logger.LogWarning("RpcClient request has been cancelled after: {Timeout}ms for request: {Request}",
                sw.ElapsedMilliseconds, JsonSerializer.Serialize(request));

            return Task.FromResult<T?>(default);
        }
    }

    /// <inheritdoc />
    protected override Task MessageHandle(object? sender, BasicDeliverEventArgs @event)
    {
        if (@event.BasicProperties.CorrelationId != Options.CorrelationId)
        {
            Logger.LogDebug("Received RPC call for another consumer: {CorrelationId}", Options.CorrelationId);

            return Task.CompletedTask;
        }

        var response = @event.GetData(Session.Config);

        var messageType = CachedReflection.GetType(@event.BasicProperties.Type);

        if (messageType is null)
        {
            Logger.LogError("Unable to find the type used inside the event message in loaded assembly types");
            return Task.CompletedTask;
        }

        if (_listeners.TryGetValue(messageType, out var listener))
        {
            listener.Add(response);
        }

        Logger.LogDebug("Rpc response received for correlationId : {CorrelationId}", Options.CorrelationId);

        return Task.CompletedTask;
    }
}