using Microsoft.ApplicationInsights.Extensibility.Implementation;
using RabbitMQ.Client;

namespace EzRabbitMQ.Extensions;

/// <summary>
/// Helpful methods for telemetry objects
/// </summary>
public static class TelemetryExtensions
{
    /// <summary>
    /// Set telemetry context using event's operation id as parent operation id.
    /// </summary>
    /// <param name="context">The telemetry context to set</param>
    /// <param name="properties">Event's properties containing headers that will be read to find operation Id</param>
    public static void SetTelemetry(this OperationContext context, IReadOnlyBasicProperties properties)
    {
        if (properties.Headers != null && properties.Headers.TryGetValue(Constants.TelemetryOperationIdHeaderName, out var operationId))
        {
            if (operationId is byte[] buffer)
            {
                var parentOperationId = Encoding.UTF8.GetString(buffer);

                if (parentOperationId == context.Id)
                {
                    context.Id = Guid.NewGuid().ToString();
                }

                context.ParentId = parentOperationId;
            }
        }
    }
}