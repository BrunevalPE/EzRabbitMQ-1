# EzRabbitMQ - Easy to use rabbitMQ .Net wrapper

<img src="https://github.com/NymCorp/EzRabbitMQ/blob/main/docs/images/logo.png" width="200"  alt="EzRabbitMQ logo"/>

[![.NET](https://github.com/NymCorp/EzRabbitMQ/actions/workflows/dotnet.yml/badge.svg)](https://github.com/NymCorp/EzRabbitMQ/actions/workflows/dotnet.yml)

The main goal of this library is simplifying the usage of **RabbitMQ dotnet client**.

To receive a message you will need a `Mailbox`, mailboxes take `IMailboxOptions` as parameter, and there are helpful implementations of `IMailboxOptions`, 
like `DirectMailboxOptions` which has default Exchange name **"amq.direct"** and preset ExchangeType = **"direct"**.

When you create a `Mailbox` you can specify the `ConsumerOptions` allowing you to configure **exchange**, **queue**, and **binding** features.

## Get Started: 

### Install

![Nuget](https://img.shields.io/nuget/dt/EzRabbitMQ)

`dotnet add package EzRabbitMQ`

### Register services EzRabbitMQ services :

```c#
// your service provider
services.AddEzRabbitMQ();
```

## Requirements:

- .Net 5 / .Net Core 6,7,8
- RabbitMQ Client version allowed from [6.4.0,]

## Usage:

### Create and receive *`direct` exchange type* messages

For direct message type the routing key of a message must match the routing key binding to a queue.

Here is an example of a mailbox of exchange type **direct** receiving message of type `DataSample`, and a 
producer sending a exchange type **direct** to the mailbox.

The event `OnMessageReceived` will be raised:

```c#
record DataSample(string Text);

IProducerService producerService; // use injection to get a IProducerService
IMailboxService mailboxService; // use injection to get a IMailboxService

using var mailbox = mailboxService.Direct<DataSample>("ROUTING KEY", "MAILBOX NAME");

mailbox.OnMessageReceived += (sender, data) => {
    // data is IMessage<DataSample>
    Console.WriteLine($"data received: {data.Text}");
};

producerService.DirectSend("ROUTING KEY", new DataSample("Example"));
```

### Create and receive *`topic` exchange type* message

Topic message routing key must follow some rules:

It must be words separated using `.`(dot) and contains at least a `#`(hash) *can replace one or more values* or a `*`(star) *can replace one value*.


```c#
using var mailbox = mailboxService.Topic<DataSample>("root.#", "ALL_UNDER_ROOT", new ConsumerOptions());

producerService.TopicSend("ROUTING KEY", new DataSample("Example"));
```

### Create and receive *`fanout` exchange type* message

Fanout exchange type doesnt need routing key, all message are like broadcast to the exchange.

```c#
using var mailbox = mailboxService.Fanout<DataSample>("FANOUT_LISTENER", new ConsumerOptions());

producerService.FanoutSend(new DataSample("Example"));
```

### Create and receive *`headers` exchange type* Message

Exchange type headers mailbox takes a XMatch parameter to configure if all headers must match or if any header that match will be received.

```c#
Dictionary<string, string> headers = new()
{
    { "type", "jpg" },
    { "format", "chart" }
};
using var mailbox = mailboxService.Headers<DataSample>(headers, XMatch.any, "JPG_RECEIVER", new ConsumerOptions());

producerService.HeadersSend(new Dictionary<string, string>(){
    {"type": "jpg"}
}, new DataSample("Example"));
```

### Create RPC client and server and send message

You can implement a `RpcServer` that will receive `IRpcRequest` from a `RpcClient`.

```c#
// your implementation or RpcServerBase
public record RpcIncrementRequest(int CurrentValue);
public record RpcIncrementResponse(int NewValue);

public class IncrementRpcServer: RpcServerBase, 
    IRpcServerHandleAsync<RpcIncrementResponse, RpcIncrementRequest>
{
    public IncrementRpcServer(ILogger<IncrementRpcServer> logger, IMailboxOptions options, ISessionService session, IProducerService producerService, ConsumerOptions consumerOptions) 
    : base(logger, options, session, producerService, consumerOptions)
    {
    }

    public Task<RpcIncrementResponse> HandleAsync(RpcIncrementRequest request, CancellationToken cancellationToken)
    {
        Logger.LogInformation("rpc received");
        return Task.FromResult(new RpcIncrementResponse(request.CurrentValue +1));
    }
}

using var rpcServer = _mailboxService.RpcServer<IncrementRpcServer>();

using var rpcClient = _mailboxService.RpcClient();

// CallAsync will return a task that will be completed when the response is received
var response = _rpcClient.Call<RpcIncrementResponse>(new RpcIncrementRequest(1));
// response.NewValue = 2
```

### Create and receive multiple messages

You can implement your own mailbox version, you will be able to implement multiple message handle to receive multiple message within the same context.

```c#
record WiseChildGiftRequest(string Name);
record NotWiseChildGiftRequest(string Name);

public class SantaMailbox : MailboxBase,
    IMailboxHandler<WiseChildGiftRequest>, // sync handle
    IMailboxHandlerAsync<NotWiseChildGiftRequest> // async handle
{
    private readonly ILogger<TodoMailbox> _logger;

    public SantaMailbox(ILogger<TodoMailbox> logger, IMailboxOptions options, ISessionService session, ConsumerOptions consumerOptions) : base(logger, options, session, consumerOptions) { }

    public void OnMessageHandle(IMessage<WiseChildGiftRequest> message)
    {
        _logger.LogInformation("Received request from wise child");
    }

    public Task OnMessageHandleAsync(IMessage<NotWiseChildGiftRequest> message, CancellationToken cancellationToken)
    {
        _logger.LogInformation("Received request from not wise child");
        return Task.CompletedTask;
    }
}

// create instance of your mailbox
var santaMailbox = mailboxService.Create<SantaMailbox>("SANTA", "SANTA_MAILBOX", consumerOptions);

// send message to SANTA_MAILBOX
producerService.DirectSend("SANTA", new DataSample("Example"));
```

## Features :

### Options validation

Provides validation on routing key rules depending on exchange type you are using.

### App Insight metrics

Custom telemetry is wired inside publish / receive event to allow you to trace events and profile,

### Fail safe server requests with retry policy

All client/server are using a polly retry policy to avoid data loose on network issues or a server restart.

### Consumer features 

Consumer options allow you to toggle on/off lot of rabbitMQ build-in features like autoAck, QueueSizeLimit, and more.

## Configuration Options

You can configure several options using the `configure` callback in the startup.

### Override Serializer/Deserializer

You can override and change the serializer / deserializer.

```c#
// your service provider
services.AddEzRabbitMQ(config =>
{
    config.ConfigureSerialization(data => \* returns bytes array*\, 
        (bytes, type) => \* returns object from bytes array*\);
});
```

### Set AppInsight InstrumentationKey

By setting a valid InstrumentationKey you will enable appInsight metrics and tracing.

```c#
// your service provider
services.AddEzRabbitMQ(config =>
{
     var ik = context.Configuration["ApplicationInsights:InstrumentationKey"];
    if (ik is not null) config.ConfigureInstrumentationKey(ik);
});
```

### Polly retry policy

You can override the retry policy.

```c#
// your service provider
services.AddEzRabbitMQ(config =>
{
    config.ConfigurePollyPolicy(Policy
        .Handle<Exception>()
        .WaitAndRetryAsync(5, retryAttempt =>
            TimeSpan.FromSeconds(Math.Pow(2, retryAttempt))
        ));
});
```

### RPC Polly retry policy

You can override the retry policy of RPC calls using :

```c#
// your service provider
services.AddEzRabbitMQ(config =>
{
    config.ConfigureRpcPollyPolicy(Policy.HandleResult<object>(d => d is null)
                            .WaitAndRetryAsync(1, i => TimeSpan.FromSeconds(Math.Pow(2, i))));
});
```

### RabbitMQ Connection

You can modify the current connectionFactory or you can create a connection and

```c#
// your service provider
services.AddEzRabbitMQ(config =>
{
    config.ConfigureConnection(c =>
    {
        c.Uri = new Uri("rabbitmq-server:15672");
        return c;
    });

    // or you can create a new connection factory

    config.ConfigureConnection(_ => new ConnectionFactory());
});
```

## ConsumerOptions

This options can add rabbitMQ arguments to enable / disable features.

ConsumerOptions default values:

#### Async Dispatcher:

You can switch between async and sync dispatcher using the configuration :

```c#
services.AddEzRabbitMQ(config =>
{
    config.IsAsyncDispatcher = true;
});
```

#### RetryCount:

You can enable retry on consumer exception by using the consumerOptions object :

```c#
var options = new ConsumerOptions
{
    AutoAck = false, // AutoAck must be false to use RetryCount
    RetryCount = 3, // After 3 retry exception in the message will be reject,
};
```

#### Durable Queue :

To create a durable queue persistant after server restart :

```c#
var options = new ConsumerOptions
{
    QueueDurable = true, // set durable feature on queue (need queue recreation if changed)
    QueueAutoDelete = false. // set auto delete feature on queue (need queue recreation if changed)
};
```

#### Durable Exchange :

To create a durable exchange persistant after server restart :

```c#
var options = new ConsumerOptions
{
    ExchangeDurable = true, // set durable feature on queue (need queue recreation if changed)
    ExchangeAutoDelete = false. // set auto delete feature on queue (need queue recreation if changed)
};
```

#### Exclusive Queue

To allow only one consumer you can enable the Exclusive feature of the queue :
```c#
var options = new ConsumerOptions
{
    QueueExclusive  = true, // set exclusive feature on queue (need queue recreation if changed)
};
```

#### Limits
To set the prefetch message count limit :

```c#
var options = new ConsumerOptions
{
    PrefetchCount = 10, // set the prefetch amount of message read by this consumer
    PrefetchGlobal = false // channel global or by consumer limit
};
```

### Exchange and Queue recreation mode
By setting the exchange recreation mode you can recreate exchange and queue if a breaking change is detected, like if you want to enable a new feature.

```c#
var options = new ConsumerOptions
{
    QueueRecreateMode = RecreateMode.RecreateIfBreakingChangeDetected | RecreateMode.RecreateIfEmpty,
    ExchangeRecreateMode = RecreateMode.RecreateIfBreakingChangeDetected
};
```

## Troubleshooting:

If you implement constructor using `ILogger`, you must provide an Type for the ILogger.

```text
System.InvalidOperationException
Unable to resolve service for type 'Microsoft.Extensions.Logging.ILogger' while attempting to activate 'EzRabbitMQ.Tests.CustomMailbox'.
```

E.g.:

```C#
public class CustomMailbox : MailboxBase
{
    public CustomMailbox(
        ILogger<CustomMailbox> logger, 
        IMailboxOptions options, 
        ISessionService session, 
        ConsumerOptions consumerOptions) 
        : base(logger, options, session, consumerOptions)
    {
    }
}
```

## License

[Open source licence Apache2](./license.txt)


## Contribution guide

All contributions are welcome.
