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
using RabbitMQ.Client.Exceptions;

namespace NLog.Logstash.RabbitMQ
{
    [Target("RabbitMQ")]
    public sealed class RabbitMqTarget : TargetWithLayout
    {
        private const string LayoutString = @"{ ""app"":""%APP%"",  ""environment"":""${json-encode:${environment:ENVIRONMENT_NAME}}"", ""machine"":""${json-encode:${machinename}}"", ""processId"":""${processid}"", ""thread"":""${json-encode:${whenEmpty:${threadname}:whenEmpty=${threadid}}}"", ""timestamp"":""${json-encode:${longdate:universalTime=true}}"", ""message"":""${json-encode:${message}}"", ""logger"":""${json-encode:${logger}}"", ""level"":""${json-encode:${level}}"" ${onexception:, ""exception""\:""${json-encode:${exception:format=ToString}}""}}";

        private static readonly object ConnectionSyncRoot = new object();

        private readonly TaskFactory _taskFactory = new TaskFactory();

        private readonly ManualResetEvent _isRestarting = new ManualResetEvent(false);
        private readonly ConcurrentQueue<LogMessage> _messageQueue = new ConcurrentQueue<LogMessage>();

        private IConnection _connection;
        private CancellationTokenSource _cancellationTokenSource;
        private bool _isShuttingDown;
        private bool _isStarted;
        private Task _restartTask;
        private volatile Task _task;
        private bool _exchangeDeclared;

        public RabbitMqTarget()
        {
            _cancellationTokenSource = new CancellationTokenSource();
            var completion = new TaskCompletionSource<object>();
            completion.SetResult(new object());
            _restartTask = completion.Task;
        }

        [RequiredParameter]
        public string ConnectionString { get; set; }

        public string App { get; set; }

        [DefaultValue("${level}")]
        public Layout Topic { get; set; }

        [DefaultValue("logging-exchange")]
        public Layout Exchange { get; set; }

        [DefaultValue(true)]
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
                InternalLogger.Warn("Sending queue is full. Message would be dropped: [{0}]", Layout.Render(logEvent));
                return;
            }

            _messageQueue.Enqueue(new LogMessage
            {
                Payload = Layout.Render(logEvent),
                Topic = Topic.Render(logEvent),
                Exchange = Exchange.Render(logEvent)
            });
            CreateWorker();
        }

        protected override void Dispose(bool disposing)
        {
            ShutDown();
            _cancellationTokenSource.Cancel();
            base.Dispose(disposing);
        }

        private static bool IsAlive(Task task)
        {
            return task != null && !(task.IsCanceled || task.IsCompleted || task.IsFaulted);
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

        private void Consume()
        {
            try
            {
                Start()
                    .ContinueWith(
                        t =>
                            {
                                var channel = OpenChannel();
                                var properties = channel.CreateBasicProperties();
                                properties.ContentEncoding = Encoding.UTF8.WebName;
                                properties.ContentType = "application/json";
                                properties.DeliveryMode = DeliveryMode;

                                var tryCount = 0;
                                while (tryCount < RetryCount)
                                {
                                    LogMessage logMessage;
                                    while (_messageQueue.TryDequeue(out logMessage))
                                    {
                                        tryCount = 0;
                                        if (!_exchangeDeclared)
                                        {
                                            channel.ExchangeDeclare(logMessage.Exchange, ExchangeType.Topic, Durable);
                                            _exchangeDeclared = true;
                                        }

                                        var messageBodyBytes = Encoding.UTF8.GetBytes(logMessage.Payload);

                                        channel.BasicPublish(logMessage.Exchange, logMessage.Topic, properties, messageBodyBytes);
                                    }

                                    tryCount++;

                                    Thread.Sleep((int)RetryDelay.TotalMilliseconds);
                                }

                                ShutDown();

                                InternalLogger.Info("All elements in queue are processed");
                            })
                    .Wait(5000);
            }
            catch (Exception ex)
            {
                InternalLogger.Error(ex.ToString());
            }
        }

        private Task Start()
        {
            if (_isStarted || _isShuttingDown)
            {
                return _task;
            }

            return Restart();
        }

        private IModel OpenChannel()
        {
            if (!_isStarted || _isShuttingDown)
            {
                throw new InvalidOperationException("RabbitMQ Connection is not ready");
            }

            if (_connection == null || !_connection.IsOpen)
            {
                throw new InvalidOperationException("RabbitMQ Connection is not open");
            }

            try
            {
                return _connection.CreateModel();
            }
            catch (Exception ex)
            {
                HandleRabbitFailure(ex);
                throw;
            }
        }

        private void Connect(CancellationToken token)
        {
            if (_isShuttingDown)
            {
                // connection is already closed - nothing to do
                return;
            }

            var factory = new ConnectionFactory
            {
                Uri = ConnectionString,
                RequestedConnectionTimeout = 5000 // 5 sec
            };

            int retryCount = 0;
            while (!token.IsCancellationRequested)
            {
                // trying to open the connection until cancellation token appear
                IConnection newConnection = null;
                try
                {
                    newConnection = factory.CreateConnection();
                    newConnection.ConnectionShutdown += OnConnectionShutdown;
                    _connection = newConnection;

                    _isStarted = true;

                    return;
                }
                catch (Exception ex)
                {
                    // failed to open the connection, starting to reconnect with increasing interval
                    // maximum interval is 10 sec
                    var secToReconnect = Math.Min(10, retryCount);
                    InternalLogger.Warn("Error: {0}. Unable to connect to RabbitMQ. Retrying in {1} seconds.", ex, secToReconnect);

                    if (newConnection != null)
                    {
                        newConnection.ConnectionShutdown -= OnConnectionShutdown;
                        newConnection.Abort(500);
                    }

                    Thread.Sleep(TimeSpan.FromSeconds(secToReconnect));
                    retryCount++;
                }
            }
        }

        /// <summary>
        /// Connection close event
        /// </summary>
        private void OnConnectionShutdown(IConnection con, ShutdownEventArgs reason)
        {
            Task.Factory.StartNew(
                () =>
                    {
                        con.ConnectionShutdown -= OnConnectionShutdown;
                        HandleRabbitFailure(new Exception("Connection was shut down"));
                    });
        }

        private void HandleRabbitFailure(Exception exception)
        {
            if (!_isStarted || _isShuttingDown)
            {
                // connection is closing, nothing to do here
                return;
            }

            InternalLogger.Error("Channel failure was detected. Trying to restart the RabbitMQ instance. {0}.", exception);
            Restart();
        }

        private Task Restart()
        {
            lock (ConnectionSyncRoot)
            {
                // restarting task is already running, nothing to do here
                if (_isRestarting.WaitOne(0) || _isShuttingDown)
                {
                    return new TaskCompletionSource<bool>().Task;
                }

                // set configuration of restarting state
                _isRestarting.Set();
            }

            InternalLogger.Trace("RabbitMQ restarting...");

            ResetRestartTask();

            // closing the connection and open again
            var token = _cancellationTokenSource.Token;
            _restartTask = Task.Factory.StartNew(DropConnection, token, TaskCreationOptions.LongRunning, TaskScheduler.Default)
                .ContinueWith(_ => Connect(token), token, TaskContinuationOptions.LongRunning, TaskScheduler.Default)
                .ContinueWith(
                    t =>
                    {
                        lock (ConnectionSyncRoot)
                        {
                            _isRestarting.Reset();
                        }

                        if (!t.IsFaulted)
                            return;

                        if (t.Exception != null)
                        {
                            throw t.Exception.InnerException;
                        }
                    },
                    token);

            return _restartTask;
        }

        private void DropConnection()
        {
            if (_connection != null)
            {
                if (_connection.CloseReason == null)
                {
                    InternalLogger.Trace("RabbitMQ closing connection.");
                    try
                    {
                        _connection.Close(500);
                        if (_connection.CloseReason != null)
                        {
                            _connection.Abort(500);
                        }
                    }
                    catch (AlreadyClosedException ex)
                    {
                        InternalLogger.Warn("Connection is already closed. Possible race condition. {0}", ex);
                    }

                    InternalLogger.Trace("RabbitMQ disposing connection.");
                }

                _connection = null;
            }
        }

        /// <summary>
        /// Waiting for restart task and cancellation of other tasks
        /// </summary>
        private void ResetRestartTask()
        {
            if (!_restartTask.IsCompleted)
            {
                _cancellationTokenSource.Cancel();

                // restart task is not completed yes. wait for it
                try
                {
                    _restartTask.Wait();
                }
                catch (AggregateException ex)
                {
                    ex.Handle(
                        e =>
                        {
                            InternalLogger.Error("Caught unexpected exception {0}", e);
                            return true;
                        });
                }
                catch (Exception ex)
                {
                    InternalLogger.Error("Caught unexpected exception {0}", ex);
                }
                finally
                {
                    _cancellationTokenSource = new CancellationTokenSource();
                }
            }
        }

        private void ShutDown()
        {
            InternalLogger.Debug("Shutting down RabbitMQ connection");
            _isShuttingDown = true;
            ResetRestartTask();

            var token = _cancellationTokenSource.Token;
            _restartTask = Task.Factory.StartNew(DropConnection, token, TaskCreationOptions.LongRunning, TaskScheduler.Default);

            _restartTask.Wait(5000);

            _isStarted = false;

            _isShuttingDown = false;
        }

        private class LogMessage
        {
            public string Payload { get; set; }

            public string Topic { get; set; }

            public string Exchange { get; set; }

            public override string ToString()
            {
                return string.Format("Payload: {0}, Topic: {1}, Exchange: {2}", Payload, Topic, Exchange);
            }
        }
    }
}
