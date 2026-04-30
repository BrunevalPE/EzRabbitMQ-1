using EzRabbitMQ.Resiliency;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Client;

namespace EzRabbitMQ;

/// <summary>
/// .Net Core 5 extensions method
/// </summary>
public static class EzRabbitMQExtensions
{
    /// <summary>
    /// Add all the services need to use EzRabbitMQ features
    /// </summary>
    /// <param name="services">Your application service collection</param>
    /// <param name="configure">Optional configure action can be pass to configure behaviors like PollyPolicies or server connection</param>
    /// <returns>IServiceCollection</returns>
    // ReSharper disable once InconsistentNaming
    public static IServiceCollection AddEzRabbitMQ(this IServiceCollection services, Action<EzRabbitMQConfig>? configure = default!)
    {
        var config = new EzRabbitMQConfig();

        configure?.Invoke(config);

        services
            .AddSingleton(config)
            .AddSingleton<ProducerExtractInformation>()
            .AddSingleton<MailboxExtractInformation>()
            .AddSingleton<ValidationService>()
            .AddSingleton<ITelemetryService, TelemetryService>()
            .AddSingleton<IProducerService, ProducerService>()
            .AddSingleton<IPollyService, PollyService>()
            .AddSingleton<IMailboxService, MailboxService>()
            .AddSingleton<IConnectionFactory>(config.Connection);

        services.TryAddTransient<IConnectionService, ConnectionService>();
        services.TryAddTransient<ISessionService, SessionService>();

        return services;
    }

#if NETCOREAPP
    /// <summary>
    /// You can useEzRabbitMQ for additional configuration options
    /// If you call UseEzRabbitMQ your configuration will be read to find an override of the log level
    /// </summary>
    /// <param name="builder"><see cref="IHostBuilder"/></param>
    /// <returns><see cref="IHostBuilder"/></returns>
    // ReSharper disable once InconsistentNaming
    public static IHostBuilder UseEzRabbitMQ(this IHostBuilder builder)
    {
        return builder.ConfigureLogging((context, logger) =>
        {
            if (context.Configuration.TryGetEnum<LogLevel>(ConfigurationKeys.LogLevelKey, out var level))
            {
                logger.SetMinimumLevel(level);
            }
        });
    }
#endif
}