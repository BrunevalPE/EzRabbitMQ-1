using EzRabbitMQ.Exceptions;
using EzRabbitMQ.Resiliency;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace EzRabbitMQ;

/// <inheritdoc />
public class SessionService : ISessionService
{
    // ReSharper disable once PrivateFieldCanBeConvertedToLocalVariable
    private readonly IPollyService _pollyService;
    private readonly CancellationTokenSource _cancellationTokenSource = new();

    /// <summary>
    /// Connection Service
    /// </summary>
    protected readonly IConnectionService Connection;

    /// <summary>
    /// Logger
    /// </summary>
    protected readonly ILogger<SessionService> Logger;

    /// <summary>
    /// Session Service
    /// </summary>
    /// <param name="logger">Logger used to log trace and debug information</param>
    /// <param name="connection">RabbitMQ connection to the server</param>
    /// <param name="pollyService">Polly service used to wrap client/server calls</param>
    /// <param name="telemetryService">Telemetry service for trace request</param>
    /// <param name="config">Library current configuration</param>
    public SessionService(
        ILogger<SessionService> logger,
        IConnectionService connection,
        IPollyService pollyService,
        ITelemetryService telemetryService,
        EzRabbitMQConfig config)
    {
        Logger = logger;
        Connection = connection;
        _pollyService = pollyService;
        Config = config;

        Telemetry = telemetryService;
        Polly = pollyService;

        _pollyService.TryExecute<CreateChannelException>(CreateChannel);
    }

    /// <inheritdoc />
    public IChannel? Model { get; private set; }

    /// <inheritdoc />
    public BasicProperties? Properties { get; private set; }

    /// <inheritdoc />
    public ITelemetryService Telemetry { get; }

    /// <inheritdoc />
    public IPollyService Polly { get; }

    /// <summary>
    /// Current library configuration
    /// </summary>
    public EzRabbitMQConfig Config { get; }

    /// <inheritdoc />
    public void SetProperties(BasicProperties properties)
    {
        Properties = properties;
    }

    /// <inheritdoc />
    public void Dispose()
    {
        try
        {
            Logger.LogDebug("Session disposed {ChannelNumber}", Model?.ChannelNumber);

            Model?.Dispose();
        }
        catch (ObjectDisposedException) // ignored
        {
        }

        Connection.Dispose();

        _cancellationTokenSource.Cancel();
    }

    /// <inheritdoc />
    public void RefreshChannelIfClosed()
    {
        if (!Model?.IsClosed ?? false) return;

        Dispose();
        Connection.TryConnect();
        CreateChannel();
    }

    /// <inheritdoc />
    public CancellationToken SessionToken => _cancellationTokenSource.Token;

    private void CreateChannel()
    {
        try
        {
            Model = Connection.Connection?.CreateChannelAsync().ConfigureAwait(false).GetAwaiter().GetResult();
        }
        catch
        {
            Model?.Dispose();
        }

        if (Model is null)
        {
            Logger.LogError("Unable to create connection channel");
            return;
        }

        Properties = new BasicProperties();

        Model.CallbackExceptionAsync -= OnCallbackExceptionAsync;
        Model.ChannelShutdownAsync -= OnChannelShutdownAsync;

        Model.CallbackExceptionAsync += OnCallbackExceptionAsync;
        Model.ChannelShutdownAsync += OnChannelShutdownAsync;

        Logger.LogDebug("Session created : {ChannelNumber}", Model.ChannelNumber);
    }

    private Task OnChannelShutdownAsync(object? sender, ShutdownEventArgs e)
    {
        if (e.ReplyCode != 200)
        {
            Logger.LogError("OnChannelShutdown trigger with message : {ReplyText}", e.ReplyText);
            return Task.CompletedTask;
        }

        Logger.LogDebug("Channel shutdown {CloseReason}, {ChannelNumber}", Model?.CloseReason, Model?.ChannelNumber);
        return Task.CompletedTask;
    }

    private Task OnCallbackExceptionAsync(object? sender, CallbackExceptionEventArgs e)
    {
        Logger.LogError(e.Exception, "RabbitMQ Channel exception occured {ChannelNumber}", Model?.ChannelNumber);
        return Task.CompletedTask;
    }
}