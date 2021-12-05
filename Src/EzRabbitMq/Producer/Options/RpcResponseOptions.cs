﻿
namespace EzRabbitMQ
{
    /// <summary>
    /// RPC producer options
    /// </summary>
    public record RpcResponseOptions : IProducerOptions
    {
        /// <summary>
        /// Rpc response options
        /// </summary>
        /// <param name="routingKey">Client address.</param>
        /// <param name="correlationId">Set the correlationId for the RPC response.</param>
        public RpcResponseOptions(string routingKey, string correlationId)
        {
            RoutingKey = routingKey;
            Properties.ReplyTo = Constants.RpcReplyToQueue;
            Properties.CorrelationId = correlationId;
        }

        /// <inheritdoc />
        public string RoutingKey { get; } = Constants.RpcDefaultRoutingKey;

        /// <inheritdoc />
        public string ExchangeName => string.Empty;

        /// <inheritdoc />
        public ProducerProperties Properties { get; } = new();
    }
}