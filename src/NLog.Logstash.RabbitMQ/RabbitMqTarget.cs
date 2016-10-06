using System;
using System.Collections.Concurrent;
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

        private Layout _topic = "${level}";
        private Layout _exchange = "logging-exchange";
        private bool _durable = true;
        private int _retryCount = 4;
        private int _retryDelay = 500;
        private RabbitMqDeliveryMode _deliveryMode = RabbitMqDeliveryMode.Persistent;
        private int _maxMessageCount = 10000;
        private int _connectionRetryDelay = 1000;

        private Task _consumingTask;
        private readonly ConcurrentQueue<LogMessage> _logQueue = new ConcurrentQueue<LogMessage>();
        private readonly TaskFactory _consumingTaskFactory = new TaskFactory();
        
        [RequiredParameter]
        public string App { get; set; }

        [RequiredParameter]
        public string ConnectionString { get; set; }

        public Layout Topic
        {
            get { return _topic; }
            set { _topic = value; }
        }

        public Layout Exchange
        {
            get { return _exchange; }
            set { _exchange = value; }
        }

        public bool Durable
        {
            get { return _durable; }
            set { _durable = value; }
        }

        public int RetryCount
        {
            get { return _retryCount; }
            set { _retryCount = value; }
        }

        public int RetryDelay
        {
            get { return _retryDelay; }
            set { _retryDelay = value; }
        }

        public int ConnectionRetryDelay
        {
            get { return _connectionRetryDelay; }
            set { _connectionRetryDelay = value; }
        }

        public RabbitMqDeliveryMode DeliveryMode
        {
            get { return _deliveryMode; }
            set { _deliveryMode = value; }
        }

        public int MaxMessageCount
        {
            get { return _maxMessageCount; }
            set { _maxMessageCount = value; }
        }

        protected override void InitializeTarget()
        {
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

        protected override void Write(AsyncLogEventInfo logEvent)
        {
            Write(logEvent.LogEvent);
        }

        protected override void Write(LogEventInfo logEvent)
        {
            CheckConsumerTask();

            if (_logQueue.Count >= MaxMessageCount)
            {
                InternalLogger.Info(string.Format("Internal queue is full. Message would be dropped: [{0}]", Layout.Render(logEvent)));

                return;
            }

            _logQueue.Enqueue(new LogMessage(Layout.Render(logEvent), Topic.Render(logEvent), Exchange.Render(logEvent)));
        }

        private void CheckConsumerTask()
        {
            try
            {
                if (IsTaskAlive(_consumingTask))
                {
                    return;
                }

                lock (SyncRoot)
                {
                    if (IsTaskAlive(_consumingTask))
                    {
                        return;
                    }

                    _consumingTask = _consumingTaskFactory.StartNew(Consume);
                }
            }
            catch (Exception ex)
            {
                InternalLogger.Error(ex.ToString());
            }
        }

        private static bool IsTaskAlive(Task task)
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
                    var properties = channel.CreateBasicProperties();
                    properties.ContentEncoding = Encoding.UTF8.WebName;
                    properties.ContentType = "application/json";
                    properties.DeliveryMode = (byte)DeliveryMode;

                    var exchangeIsDeclared = false;

                    var tryCount = 0;
                    while (tryCount < RetryCount)
                    {
                        LogMessage logMessage;

                        while (_logQueue.TryDequeue(out logMessage))
                        {
                            tryCount = 0;

                            if (!exchangeIsDeclared)
                            {
                                channel.ExchangeDeclare(logMessage.Exchange, ExchangeType.Topic, Durable);
                                exchangeIsDeclared = true;
                            }

                            var messageBody = Encoding.UTF8.GetBytes(logMessage.Message);

                            channel.BasicPublish(logMessage.Exchange, logMessage.Topic, properties, messageBody);
                        }

                        tryCount++;

                        Thread.Sleep(RetryDelay);
                    }

                    channel.Close();
                    connection.Close();
                }

                InternalLogger.Info("Internal queue processing is finished");
            }
            catch (Exception exception)
            {
                InternalLogger.Error(exception.ToString());
                
                // 1 sec connection retry delay
                Thread.Sleep(ConnectionRetryDelay);
            }
        }
    }
}