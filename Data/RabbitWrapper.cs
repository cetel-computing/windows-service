using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using WindowsService.Config;

namespace WindowsService.Data
{
    public class RabbitWrapper : IDisposable
    {
        public delegate void LogOutputHandler(object sender, string output);
        public delegate void MessageReceivedHandler(BasicDeliverEventArgs e, string ackTag);

        private const int ReconnectionInterval = 5000;

        private readonly object _lockObj = new object();

        private IModel _channel;
        private IConnection _connection;
        private EventingBasicConsumer _consumer;

        private volatile bool _disposing, _reconnecting, _consuming;

        private IBasicProperties _props;

        public ConsumerSettings Settings { get; }

        public RabbitWrapper(ConsumerSettings settings)
        {
            Settings = settings;
        }

        public void Dispose()
        {
            GC.SuppressFinalize(this);
            Dispose(true);
        }

        public event MessageReceivedHandler MessageReceived;
        public event LogOutputHandler OnLogOutput;

        /// <summary>
        ///     Initiates a connection.
        /// </summary>
        public void Initialise()
        {
            TryConnect(false);
        }

        /// <summary>
        ///     Begins queue consumption.
        /// </summary>
        public void Consume()
        {
            _channel.QueueDeclare(Settings.Queue, Settings.Persistent, false, false, null);
            _channel.ExchangeDeclare(Settings.Exchange, ExchangeType.Topic, Settings.Persistent);

            if (Settings.RoutingKeys != null)
                foreach (var k in Settings.RoutingKeys)
                    _channel.QueueBind(Settings.Queue, Settings.Exchange, k, null);

            if (Settings.Qos > 0)
                _channel.BasicQos(0, Settings.Qos, false);

            if (Settings.Persistent)
            {
                _props = _channel.CreateBasicProperties();
                _props.Persistent = true;
            }

            Start();
        }

        private void Connect()
        {
            try
            {
                _channel?.Dispose();
            }
            catch
            {
            }

            _channel = null;

            try
            {
                _connection?.Dispose();
            }
            catch
            {
            }

            _connection = null;

            var f = new ConnectionFactory
            {
                Uri = new Uri(Settings.Uri),
                RequestedHeartbeat = TimeSpan.FromSeconds(30),
                AutomaticRecoveryEnabled = true,
                TopologyRecoveryEnabled = true,
                NetworkRecoveryInterval = TimeSpan.FromSeconds(5),
            };

            _connection = f.CreateConnection();

            _connection.ConnectionShutdown += OnConnectionShutdown;
            _connection.ConnectionBlocked += OnConnectionBlocked;
            _connection.ConnectionUnblocked += OnConnectionUnblocked;

            _channel = _connection.CreateModel();
            _channel.ModelShutdown += _channel_ModelShutdown;
            _channel.CallbackException += ChannelOnCallbackException;

            _consumer = null;
            _consuming = false;
        }

        private void ChannelOnCallbackException(object sender, CallbackExceptionEventArgs e)
        {
            Log($"[{Thread.CurrentThread.ManagedThreadId}] RabbitMQ: Channel callback exception. {e.Exception}");

            Task.Factory.StartNew(() => TryConnect(true));
        }

        private void _channel_ModelShutdown(object sender, ShutdownEventArgs e)
        {
            Log($"[{Thread.CurrentThread.ManagedThreadId}] RabbitMQ: Model shutdown occurred. {e.ReplyText}");

            Task.Factory.StartNew(() => TryConnect(true));
        }

        private void HandleMessage(object model, BasicDeliverEventArgs e)
        {
            MessageReceived?.Invoke(e, $"{e.ConsumerTag}|{e.DeliveryTag}");
        }

        private void OnConnectionBlocked(object sender, ConnectionBlockedEventArgs e)
        {
            Log($"[{Thread.CurrentThread.ManagedThreadId}] RabbitMQ: Connection was blocked. {e.Reason}");

            lock (_lockObj)
            {
                Monitor.Wait(_lockObj);
            }
        }

        private void OnConnectionUnblocked(object sender, EventArgs e)
        {
            Log($"[{Thread.CurrentThread.ManagedThreadId}] RabbitMQ: Connection was unblocked.");

            Monitor.Pulse(_lockObj);
        }

        private void OnConnectionShutdown(object sender, ShutdownEventArgs e)
        {
            Log($"[{Thread.CurrentThread.ManagedThreadId}] RabbitMQ: Connection shutdown occurred. {e}");

            Task.Factory.StartNew(() => TryConnect(true));
        }

        private bool TryConnect(bool consume)
        {
            if (_disposing)
            {
                Log($"[{Thread.CurrentThread.ManagedThreadId}] RabbitMQ: Wrapper disposing. Aborting reconnect (1).");
                // A connection shutdown event is received on Dispose()
                return false; // so ignore it if necessary
            }

            lock (_lockObj)
            {
                // Only try reconnecting from one thread at a time
                if (_reconnecting)
                {
                    Log($"[{Thread.CurrentThread.ManagedThreadId}] RabbitMQ: Another reconnect in progress. Aborting.");
                    return true;
                }

                if ((_connection?.IsOpen ?? false) && (_channel?.IsOpen ?? false))
                {
                    Log($"[{Thread.CurrentThread.ManagedThreadId}] RabbitMQ: Connection already established, aborting reconnect. Connection: {_connection?.IsOpen ?? false}, Channel: {_channel?.IsOpen ?? false}.");
                    return true; // A connection has already been established
                }

                _reconnecting = true;

                var host = new Uri(Settings.Uri).Host;

                do
                {
                    if (_disposing)
                    {
                        Log($"[{Thread.CurrentThread.ManagedThreadId}] RabbitMQ: Wrapper disposing. Aborting reconnect (2).");
                        return false;
                    }

                    try
                    {
                        Log($"[{Thread.CurrentThread.ManagedThreadId}] RabbitMQ: Trying to connect to RabbitMQ at '{host}'.");

                        Connect();

                        Log($"[{Thread.CurrentThread.ManagedThreadId}] RabbitMQ: Connected to RabbitMQ at '{host}'.");

                        if (consume)
                        {
                            Log($"[{Thread.CurrentThread.ManagedThreadId}] RabbitMQ: Resuming queue consumption.");

                            Consume();

                            Log($"[{Thread.CurrentThread.ManagedThreadId}] RabbitMQ: Resumed.");
                        }

                        _reconnecting = false;

                        return true;
                    }
                    catch (Exception ex)
                    {
                        Serilog.Log.Error(ex, $"[{Thread.CurrentThread.ManagedThreadId}] RabbitMQ: Connect exception.");
                        Thread.Sleep(ReconnectionInterval);
                    }
                } while (true);
            }
        }

        public void BasicAck(string tag, bool multiple)
        {
            try
            {
                lock (_lockObj)
                {
                    var tags = tag.Split('|');

                    if (tags[0] != _consumer.ConsumerTags.FirstOrDefault()) //If the consumer tag has changed, then don't try to ACK as it will close the connection.
                        return;

                    _channel.BasicAck(ulong.Parse(tags[1]), multiple);
                }
            }
            catch (RabbitMQ.Client.Exceptions.AlreadyClosedException e)
            {
                Serilog.Log.Error(e, $"[{Thread.CurrentThread.ManagedThreadId}] RabbitMQ: Client ACK exception. Reconnecting.");

                TryConnect(true);

                throw;
            }
            catch (Exception e)
            {
                Serilog.Log.Error(e, $"[{Thread.CurrentThread.ManagedThreadId}] RabbitMQ: ACK exception");

                throw;
            }
        }

        public void BasicPublish(string exchange, string routingKey, byte[] body)
        {
            BasicPublishInternal(exchange, routingKey, body, true);
        }

        private void BasicPublishInternal(string exchange, string routingKey, byte[] body, bool retry)
        {
            lock (_lockObj)
            {
                try
                {
                    _channel.BasicPublish(exchange, routingKey, _props, body);
                }
                catch (Exception e)
                {
                    if (!ExceptionFilter(e))
                        throw;

                    if (/*TryConnect(true)&&*/  retry)
                        BasicPublishInternal(exchange, routingKey, body, false);
                }
            }
        }

        private static bool ExceptionFilter(Exception e)
        {
            var ns = e.GetType().Namespace;

            switch (ns)
            {
                case "System.IO":
                case "System.Net":
                case "RabbitMQ.Client.Exceptions":
                    return true;
                default:
                    return false;
            }
        }

        private void Log(string output)
        {
            OnLogOutput?.Invoke(this, output);
        }

        /// <summary>
        ///     Starts receiving new messages, following a <see cref="Stop()"/>.
        /// </summary>
        public void Start()
        {
            if (_channel == null || _disposing)
            {
                return;
            }

            lock (_lockObj)
            {
                if (_consumer == null)
                {
                    _consuming = false;
                    _consumer = new EventingBasicConsumer(_channel);
                    _consumer.Received += HandleMessage;
                }

                if (!_consuming)
                {
                    _channel.BasicConsume(Settings.Queue, false, _consumer);
                    _consuming = true;
                }
            }
        }

        /// <summary>
        ///     Stops receiving new messages, keeping
        ///     the connection and channel open.
        /// </summary>
        public void Stop()
        {
            if (_consumer == null)
                return;

            lock (_lockObj)
            {
                if (!(_channel?.IsClosed ?? false) && _consuming)
                {
                    _consuming = false;
                    _channel?.BasicCancel(_consumer.ConsumerTags.FirstOrDefault());
                }
            }
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposing) return;

            _disposing = true;

            //Stop();

            _consumer = null;
            _consuming = false;
            _channel?.Abort();
            _channel?.Dispose();
            _connection?.Dispose();
        }
    }
}
