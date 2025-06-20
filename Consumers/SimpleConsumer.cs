using System.Collections.Concurrent;
using System.Collections.Specialized;
using System.Text;
using WindowsService.Config;
using WindowsService.Data;
using WindowsService.Helpers;
using WindowsService.Models;
using Newtonsoft.Json;
using RabbitMQ.Client.Events;

namespace WindowsService
{
    public abstract class SimpleConsumer : Consumer
    {
        private readonly ConcurrentDictionary<string, OutstandingMessage> _localOutstandingMessages;
        private readonly OrderedDictionary _localProcessingQueue;
        private readonly ushort _maxConcurrentMessages;
        private readonly IList<Thread> _workerThreads;
        private bool _wrapperStopped;

        protected SimpleConsumer(string crcEventName, ResultsConsumerConfig config, ushort maxConcurrentMessages = 5) : base(
            crcEventName, config)
        {
            _maxConcurrentMessages = maxConcurrentMessages;
            _localProcessingQueue = new OrderedDictionary();
            _localOutstandingMessages = new ConcurrentDictionary<string, OutstandingMessage>();
            _workerThreads = new List<Thread>();

            for (var i = 0; i < _maxConcurrentMessages; i++)
            {
                var t = new Thread(ThreadWorker);
                _workerThreads.Add(t);
            }
        }

        /// <summary>
        ///     bind the named queue to the provided exchange and start consuming
        /// </summary>
        /// <param name="channel"></param>
        /// <param name="exchange"></param>
        /// <param name="queueprefix"></param>
        public override void Consume(RabbitWrapper wrapper, string exchange, string queueprefix)
        {
            foreach (var thread in _workerThreads) thread.Start();

            // define this queue name
            _queueName = queueprefix + GetType().Name.ToLower().Replace("consumer", "");
            _wrapper = wrapper;

            _wrapper.Settings.Qos = (ushort)(_maxConcurrentMessages * 2);
            _wrapper.Settings.Queue = _queueName;
            _wrapper.Settings.Persistent = true;
            _wrapper.Settings.RoutingKeys = GetRoutingKeys();

            _wrapper.OnLogOutput += (s, o) => { Logger.Information(o); };
            _wrapper.MessageReceived += MessageReceived;

            _wrapper.Consume();

            Logger.Information($"[Consumer] Connected to queue {_queueName}, waiting for messages...");
        }

        /// <summary>
        ///     rabbitmq server delivers messages from the queue asynchronously so use a callback
        /// </summary>
        /// <param name="body"></param>
        /// <param name="tag"></param>
        protected override void MessageReceived(BasicDeliverEventArgs e, string ackTag)
        {
            string bodyStr,
                resultId = bodyStr = null;

            bool removeFromLocalQueue = false;
            try
            {
                bodyStr = Encoding.UTF8.GetString(e.Body.Span);
                var data = JsonConvert.DeserializeObject<ResultMetadata>(bodyStr);

                if (!ShouldProcessResult(data))
                {
                    Logger.Warning(
                        $"[{Thread.CurrentThread.ManagedThreadId}] [Consumer] [{ackTag}] Processing intentionally aborted for result: '{data.ResultId}'.");

                    return;
                }

                resultId = data.ResultId;

                lock (_localProcessingQueue)
                {
                    if (!_localProcessingQueue.Contains(data.ResultId))
                    {
                        MessagePreCache(data, ackTag);
                        _localProcessingQueue.Add(data.ResultId, new Tuple<ResultMetadata, string>(data, ackTag));

                        removeFromLocalQueue = true;

                        var oustandingMsg = new OutstandingMessage(
                            e.RoutingKey,
                            e.Body.ToArray()
                        );

                        _localOutstandingMessages.AddOrUpdate(data.ResultId, oustandingMsg);

                        Logger.Information(
                            $"[{Thread.CurrentThread.ManagedThreadId}] [Consumer] [{ackTag}] New RMQ Message: Local queue length: {_localProcessingQueue.Count}.");
                    }
                    else
                    {
                        Logger.Warning(
                            $"[{Thread.CurrentThread.ManagedThreadId}] [Consumer] [{ackTag}] New RMQ Message: Local queue already contains Result Id: {data.ResultId}. Message will not be added to queue.");
                    }

                    if (_localProcessingQueue.Count >= _wrapper.Settings.Qos)
                    {
                        Logger.Information("[{Thread.CurrentThread.ManagedThreadId}] [Consumer] Local queue limit reached. Pausing message consumption.");
                        _wrapper.Stop();
                        _wrapperStopped = true;
                    }
                }
            }
            catch (Exception ex)
            {
                Logger.Error(ex,
                    $"[{Thread.CurrentThread.ManagedThreadId}] [Consumer] [{ackTag}] Message processing error. Content:\r\n{bodyStr ?? string.Empty}");

                HandleMessageException(e.Body.ToArray(), resultId, e.RoutingKey, removeFromLocalQueue);
            }
            finally
            {
                Ack(ackTag);
            }
        }

        protected virtual bool ShouldProcessResult(ResultMetadata resultMetadata)
        {
            return true;
        }

        private void HandleMessageException(byte[] messageBody, string resultId, string routingKey, bool removeFromLocalQueue)
        {
            if (removeFromLocalQueue)
            {
                lock (_localProcessingQueue)
                {
                    if (_localProcessingQueue.Count > 0)
                    {
                        _localProcessingQueue.RemoveAt(0);
                    }
                }
            }

            if (resultId != null)
                _localOutstandingMessages.TryRemove(resultId, out _);


            Logger.Warning($"[Consumer] Re-publishing unprocessable message with Result Id: {resultId}.");
            _wrapper.BasicPublish(RcConfig.RabbitMQ.Exchange, routingKey, messageBody);
        }

        private void ThreadWorker()
        {
            try
            {
                Logger.Information($"[{Thread.CurrentThread.ManagedThreadId}] Queue worker start.");
                while (!CancellationTokenSource.IsCancellationRequested)
                {
                    Tuple<ResultMetadata, string> target = null;

                    try
                    {
                        lock (_localProcessingQueue)
                        {
                            if (_localProcessingQueue.Count > 0)
                            {
                                target = (Tuple<ResultMetadata, string>)_localProcessingQueue[0];
                                _localProcessingQueue.RemoveAt(0);
                            }
                        }

                        if (target != null)
                        {
                            ProcessMessage(target.Item1, target.Item2, CancellationTokenSource.Token);
                        }
                    }
                    catch (Exception ex)
                    {
                        Logger.Error(ex, $"[{Thread.CurrentThread.ManagedThreadId}] Exception processing message.");
                    }
                    finally
                    {
                        if (target != null && !CancellationTokenSource.IsCancellationRequested)
                        {
                            lock (_localProcessingQueue)
                            {
                                _localOutstandingMessages.TryRemove(target.Item1.ResultId, out _);
                            }
                        }
                    }

                    if (!CancellationTokenSource.IsCancellationRequested && _localProcessingQueue.Count < 1)
                    {
                        if (_wrapperStopped)
                            lock (_localProcessingQueue)
                            {
                                if (_wrapperStopped)
                                {
                                    try
                                    {
                                        Logger.Information($"[{Thread.CurrentThread.ManagedThreadId}] [Consumer] Local queue depleted. Resuming message consumption.");
                                        _wrapper.Start();
                                        _wrapperStopped = false;
                                    }
                                    catch (Exception ex)
                                    {
                                        Logger.Error(ex, $"[{Thread.CurrentThread.ManagedThreadId}] Failure resuming message consumption.");
                                    }
                                }
                            }

                        Thread.Sleep(100);
                    }
                }
            }
            catch (OperationCanceledException)
            {
            }
            catch (Exception ex)
            {
                Logger.Error(ex, $"[{Thread.CurrentThread.ManagedThreadId}] Queue worker exception.");
            }
            finally
            {
                Logger.Information($"[{Thread.CurrentThread.ManagedThreadId}] Queue worker stop.");
            }
        }

        private void ProcessMessage(ResultMetadata resultMetadata, string tag, CancellationToken cancellationToken)
        {
            Logger.Information($"[{Thread.CurrentThread.ManagedThreadId}] Received message with tag {tag}.");

            // message body is the file metadata object in json format so
            // attempt to convert the received message for processing
            try
            {
                //consumers must implement the process function to deal with this message
                if (resultMetadata != null)
                {
                    ProcessResult(resultMetadata, tag, cancellationToken);
                }
            }
            catch (Exception e)
            {
                Logger.Error(e,
                    $"An exception occurred processing the following result ({tag}): {JsonConvert.SerializeObject(resultMetadata)}");
            }
        }

        public override void Stop()
        {
            Logger.Information("[Consumer] Stopping.");

            _wrapper.Stop();
            _wrapper.MessageReceived -= MessageReceived;

            CancellationTokenSource.Cancel();

            foreach (var thread in _workerThreads) thread.Join();

            lock (_localProcessingQueue)
            {
                foreach (var kvp in _localOutstandingMessages)
                {
                    Logger.Information($"[Consumer] Re-publishing outstanding Result Id: {kvp.Key}.");
                    _wrapper.BasicPublish(RcConfig.RabbitMQ.Exchange, kvp.Value.RoutingKey, kvp.Value.Body);
                }
            }

            _wrapper.Dispose();

            ClearCache();
        }

        protected virtual void MessagePreCache(ResultMetadata resultMetadata, string tag)
        { }

        protected virtual void ClearCache()
        { }
    }
}
