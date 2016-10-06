using System;
using System.Collections.Concurrent;
using System.ComponentModel;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NLog.Common;
using NLog.Config;
using NLog.Layouts;
using NLog.Targets;
using RabbitMQ.Client;

namespace NLog.Logstash.RabbitMQ
{
    [Target("RabbitMQ")]
    public class RabbitMqTarget : TargetWithLayout
    {
        private const string LayoutString = @"{ ""app"":""%APP%"",  ""environment"":""${json-encode:${environment:ENVIRONMENT_NAME}}"", ""machine"":""${json-encode:${machinename}}"", ""processId"":""${processid}"", ""thread"":""${json-encode:${whenEmpty:${threadname}:whenEmpty=${threadid}}}"", ""timestamp"":""${json-encode:${longdate:universalTime=true}}"", ""message"":""${json-encode:${message}}"", ""logger"":""${json-encode:${logger}}"", ""level"":""${json-encode:${level}}"" ${onexception:, ""exception""\:""${json-encode:${exception:format=ToString}}""}}";

        private class LogMessage
        {
            public string Message;
            public string Topic;
            public string Exchange;
        }

        private Task _task;
        private readonly ConcurrentQueue<LogMessage> _messageQueue = new ConcurrentQueue<LogMessage>();
        private readonly TaskFactory _taskFactory = new TaskFactory();

        public string App { get; set; }

        [DefaultValue("${level}")]
        public Layout Topic { get; set; }

        [DefaultValue("logging-exchange")]
        public Layout Exchange { get; set; }

        [RequiredParameter]
        public string ConnectionString { get; set; }

        public bool Durable { get; set; }

        [DefaultValue("4")]
        public int RetryCount { get; set; }

        [DefaultValue("00:00:00.500")]
        public TimeSpan RetryDelay { get; set; }

        [DefaultValue("2")]
        public byte DeliveryMode { get; set; }

        [DefaultValue(10000)]
        public int MaxMessageCount { get; set; }

        protected override void InitializeTarget()
        {
            RetryCount = 4;
            RetryDelay = TimeSpan.FromMilliseconds(500);
            DeliveryMode = 2;
            MaxMessageCount = 10000;
            Topic = "${level}";
            Exchange = "logging-exchange";
            Durable = true;

            if (!string.IsNullOrEmpty(App))
            {
                Layout = LayoutString.Replace("%APP%", App);
            }

            if (Layout == null && string.IsNullOrEmpty(App))
            {
                throw new NLogConfigurationException("Either \"App\" or \"Layout\" paramameter must be specified in configuration file for RabbitMQ target");
            }

            base.InitializeTarget();
        }

        protected override void Write(LogEventInfo logEvent)
        {
            if (_messageQueue.Count >= MaxMessageCount)
            {
                InternalLogger.Info(string.Format("Internal queue is full. Message would be dropped: [{0}]", Layout.Render(logEvent)));

                return;
            }
            _messageQueue.Enqueue(new LogMessage
            {
                Message = Layout.Render(logEvent),
                Topic = Topic.Render(logEvent),
                Exchange = Exchange.Render(logEvent)
            });


            CreateWorker();
        }

        private void CreateWorker()
        {
            try
            {
                if (IsAlive(_task))
                {
                    return;
                }

                lock (SyncRoot)
                {
                    if (IsAlive(_task))
                    {
                        return;
                    }

                    _task = _taskFactory.StartNew(Consume);
                }
            }
            catch (Exception ex)
            {
                InternalLogger.Error(ex.ToString());
            }
        }

        private static bool IsAlive(Task task)
        {
            return task != null && !(task.IsCanceled || task.IsCompleted || task.IsFaulted);
        }

        private void Consume()
        {
            try
            {
                var factory = new ConnectionFactory
                {
                    Uri = ConnectionString
                };

                using (var connection = factory.CreateConnection())
                using (var channel = connection.CreateModel())
                {

                    var tryCount = 0;
                    while (tryCount < RetryCount)
                    {
                        LogMessage logMessage;

                        while (_messageQueue.TryDequeue(out logMessage))
                        {
                            tryCount = 0;

                            var properties = channel.CreateBasicProperties();

                            properties.ContentEncoding = Encoding.UTF8.WebName;

                            properties.ContentType = "application/json";

                            properties.DeliveryMode = DeliveryMode;

                            channel.ExchangeDeclare(logMessage.Exchange, ExchangeType.Topic, Durable);

                            var messageBodyBytes = Encoding.UTF8.GetBytes(logMessage.Message);

                            channel.BasicPublish(logMessage.Exchange, logMessage.Topic, properties, messageBodyBytes);
                        }

                        tryCount++;

                        Thread.Sleep((int)RetryDelay.TotalMilliseconds);
                    }

                    channel.Close();
                    connection.Close();
                }

                InternalLogger.Info("Internal queue processing is finished");
            }
            catch (Exception ex)
            {
                InternalLogger.Error(ex.ToString());
            }
        }
    }
}