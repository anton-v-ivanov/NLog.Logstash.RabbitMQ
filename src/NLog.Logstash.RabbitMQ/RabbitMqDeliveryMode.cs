namespace NLog.Logstash.RabbitMQ
{
    public enum RabbitMqDeliveryMode : byte
    {
        NonPersistent = 1,
        Persistent = 2
    }
}