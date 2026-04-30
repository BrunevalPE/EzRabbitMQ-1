using EzRabbitMQ.Extensions;
using EzRabbitMQ.Reflection;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace EzRabbitMQ;

/// <summary>
/// Rpc Server base implementation, it receive message on an exclusive queue <br/>
/// This class search in the real implementation a matching Handle method <br/>
/// using cached reflection and invoke your handle implementation
/// </summary>
public abstract class RpcServerBase : MailboxBase
{
    private const string HandleName = nameof(IRpcServerHandle<EmptyRpcResponse, EmptyRpcRequest>.Handle);
    private const string HandleAsyncName = nameof(IRpcServerHandleAsync<EmptyRpcResponse, EmptyRpcRequest>.HandleAsync);

    /// <inheritdoc />
    protected RpcServerBase(
        ILogger logger,
        IMailboxOptions options,
        ISessionService session,
        ConsumerOptions consumerOptions
    )
        : base(logger, options, session, consumerOptions)
    {
    }

    /// <inheritdoc />
    protected override async Task MessageHandle(object? sender, BasicDeliverEventArgs @event)
    {
        var replyProps = new BasicProperties
        {
            CorrelationId = @event.BasicProperties.CorrelationId
        };

        var messageType = @event.BasicProperties.Type;

        var currentType = GetType();
        var method = CachedReflection.FindMethodToInvoke(currentType, messageType, HandleName);
        var asyncMethod = CachedReflection.FindMethodToInvoke(currentType, messageType, HandleAsyncName);

        var obj = @event.GetData(Session.Config);

        object? response = null;
        if (asyncMethod is not null)
        {
            var task = asyncMethod.Invoke(this, new[] {obj, Session.SessionToken});
            if (task is not null)
            {
                response = await (dynamic) task;
            }
        }
        else if (method is not null)
        {
            response = method.Invoke(this, new[] {obj});
        }

        if (response is not null && Session.Model is not null)
        {
            var props = new BasicProperties
            {
                CorrelationId = @event.BasicProperties.CorrelationId,
                Type = response.GetType().AssemblyQualifiedName
            };
            var body = Session.Config.SerializeData(response);

            await Session.Model.BasicPublishAsync("", @event.BasicProperties.ReplyTo, false, props, new ReadOnlyMemory<byte>(body));
        }
    }
}