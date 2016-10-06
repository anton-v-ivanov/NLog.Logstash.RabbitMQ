namespace NLog.Logstash.RabbitMQ
{
    internal class LogMessage
    {
        public string Message { get; set; }
        public string Topic { get; set; }
        public string Exchange { get; set; }

        public LogMessage(string message, string topic, string exchange)
        {
            Message = message;
            Topic = topic;
            Exchange = exchange;
        }
    }
}