using System.Net;
using WindowsService.Config;
using WindowsService.Data;
using WindowsService.Models;
using RabbitMQ.Client.Events;
using Serilog;
using Serilog.Core;

namespace WindowsService
{
    public abstract class Consumer
    {
        protected EventingBasicConsumer _consumer;
        protected string _queueName;
        protected RabbitWrapper _wrapper;
        protected ResultsConsumerConfig RcConfig;
        protected Logger EventLogger;
        protected LoggerConfiguration EventLoggingConfig;
        protected Logger Logger;
        protected LoggerConfiguration LoggingConfig;
        protected HttpClient _resultClient;
        protected string _sqlConnection;
        protected string _sqlDb;

        protected static DateTime Epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        protected CancellationTokenSource CancellationTokenSource { get; }

        private readonly string _test;

        protected Consumer(string rcEventName, ResultsConsumerConfig rcConfig)
        {
            CancellationTokenSource = new CancellationTokenSource();
            RcConfig = rcConfig;
            InitLogging(RcConfig.LogFilePath);

            _test = rcEventName;

            // ------- SQL SET UP
            _sqlConnection = $"host={string.Join(",", rcConfig.SQLConfigOptions.Hosts.ToList())};" +
                $"port=3306;user id={rcConfig.SQLConfigOptions.Username};" +
                $"password={rcConfig.SQLConfigOptions.Password};" +
                $"database={rcConfig.SQLConfigOptions.Database};";

            _sqlDb = rcConfig.SQLConfigOptions.Database;

            var handler = new HttpClientHandler()
            {
                Proxy = WebRequest.DefaultWebProxy,
            };
            _resultClient = new HttpClient(handler);
        }

        public abstract bool ProcessResult(ResultMetadata resultMetadata, string tag,
            CancellationToken cancellationToken);

        /// <summary>
        ///     *IMPORTANT*
        ///     All RCs must implement this method, it is the definition of the routing key to which it binds
        /// </summary>
        /// <returns></returns>
        protected abstract string[] GetRoutingKeys();

        protected abstract void MessageReceived(BasicDeliverEventArgs e, string ackTag);

        public abstract void Consume(RabbitWrapper channel, string exchange, string queueprefix);

        /// <summary>
        ///     Implementing types can call this method in order to create an instance of a custom logger
        ///     it does not HAVE to be called though if a RC did not want logging
        /// </summary>
        protected void CreateLogger()
        {
            Console.WriteLine($"CreateLogger() - {_test}");

            //create the serilog logger
            Logger = LoggingConfig.CreateLogger();

            Logger.Information("Configured");
        }

        private void InitLogging(string logFilePath)
        {
            //set up the basic log configuration for all consumers (consumers can augment with other writeto paths')
            LoggingConfig = new LoggerConfiguration();

            //add console logging
            LoggingConfig.WriteTo.Console(outputTemplate:
                "[{Timestamp:HH:mm:ss} {Level:u3}] [" + GetType().Name + "] {Message}{Exception}{NewLine}");

            //add a standard rolling log file for this consumer
            LoggingConfig.WriteTo.File(Path.Combine(logFilePath, GetType().Name, "log.txt"), rollingInterval: RollingInterval.Day);
        }

        public abstract void Stop();

        protected virtual void Ack(string tag)
        {
            Logger.Information($"[{Thread.CurrentThread.ManagedThreadId}] ACK: {tag}.");
            _wrapper.BasicAck(tag, false);
        }
    }
}
