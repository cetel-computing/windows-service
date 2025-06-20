using System.Reflection;
using System.Text.Json;
using WindowsService.Config;
using WindowsService.Data;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Hosting.WindowsServices;
using Serilog;

namespace WindowsService
{
    public class ResultsService : IHostedService
    {
        private readonly ResultsConsumerConfig _rcConfig;

        /// <summary>
        ///     The message consumer implementations
        /// </summary>
        private readonly List<Consumer> _rcStore;

        public ResultsService()
        {
            // Read config file
            _rcConfig = LoadConfig();

            //set logging
            Log.Logger = new LoggerConfiguration()
                .WriteTo.File(Path.Combine(_rcConfig.LogFilePath, "log.txt"), rollingInterval: RollingInterval.Day)
                .WriteTo.Console(
                    outputTemplate: "[{Timestamp:HH:mm:ss} {Level:u3}] [CRC Manager] {Message}{Exception}{NewLine}")
                .CreateLogger();

            Log.Information("Initialising");

            _rcStore = [];
            InitConsumerAssemblies();
        }

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            Start();
        }

        public async Task StopAsync(CancellationToken cancellationToken)
        {
            Stop();
        }

        /// <summary>
        ///     Utility function to dynamically load all consumers via their assemblies and instantiate each object
        /// </summary>
        /// <returns></returns>
        private void InitConsumerAssemblies()
        {
            Log.Information("Loading the results consumers...");

            var consumerRefs = new List<string>
            {
                { "name" }
            };

            Log.Information($"{consumerRefs.Count} consumer(s) found");

            //instantiate each consumer
            foreach (var consumerName in consumerRefs)
            {
                Log.Information($"-- {consumerName} --");

                // instantiate the consumer instances and add to the results store to allow it to be managed - add multiple consumers here
                var consumerInstance = (Consumer)Activator.CreateInstance(typeof(SysInfoConsumer), _rcConfig);
                _rcStore.Add(consumerInstance);                
            }
        }

        /// <summary>
        ///     Starts all the consumers
        /// </summary>
        public void Start()
        {
            try
            {
                //start each consumer
                foreach (var crc in _rcStore)
                {
                    var wrapper = ConnectToRabbitMq();

                    wrapper.OnLogOutput += LogOutputHandler;

                    wrapper.Initialise();

                    wrapper.OnLogOutput -= LogOutputHandler;

                    crc.Consume(wrapper, _rcConfig.RabbitMQ.Exchange, _rcConfig.RabbitMQ.QueuePrefix);
                }

                Log.Information("Successfully started");
            }
            catch (Exception e)
            {
                Log.Error(e, "Failed to start.");
            }
        }

        private void LogOutputHandler(object sender, string message)
        {
            Log.Information(message);
        }

        /// <summary>
        ///     Stops all the consumers
        /// </summary>
        public void Stop()
        {
            try
            {
                Log.Information("Stop requested.");

                var stopTasks = _rcStore.Select(rc =>
                {
                    Log.Information($"Stopping '{rc.GetType().Name}'.");
                    return Task.Factory.StartNew(rc.Stop);
                }).ToArray();
                Task.WaitAll(stopTasks);

                Log.Information("Successfully stopped");
            }
            catch (Exception e)
            {
                Log.Error($"Failed to stop {e.Message}");
            }
        }

        private RabbitWrapper ConnectToRabbitMq()
        {
            string rabbitHost = _rcConfig.RabbitMQ.Hostname,
                rabbitVirtualHost = _rcConfig.RabbitMQ.VirtualHost,
                user = _rcConfig.RabbitMQ.Username,
                pass = _rcConfig.RabbitMQ.Password;

            var port = _rcConfig.RabbitMQ.Port;

            var uri = $"amqp://{user}:{pass}@{rabbitHost}:{port}/{rabbitVirtualHost}";

            var cs = new ConsumerSettings
            {
                Uri = uri,
                Exchange = _rcConfig.RabbitMQ.Exchange
            };

            return new RabbitWrapper(cs);
        }

        public static ResultsConsumerConfig LoadConfig()
        {
            var filename = $"appsettings.json";
            var path = Assembly.GetExecutingAssembly().Location;

            if (WindowsServiceHelpers.IsWindowsService())
            {
                // We are running as a Windows service, so the working folder will be %WINDIR%\System32.
                // Therefore, we set the content root to the executable folder instead.
                // This will ensure that, for example, configuration files are loaded from the application folder.
                path = Environment.ProcessPath;
            }

            var dir = Path.GetDirectoryName(path);
            var confFile = Path.Combine(dir, filename);

            if (!File.Exists(confFile))
            {
                Log.Logger.Error($"Config file not found: '{path}'.");
                throw new Exception($"Config file not found: '{path}'.");
            }

            var json = File.ReadAllText(confFile);

            var config = JsonSerializer.Deserialize<ResultsConsumerConfig>(json);

            return config;
        }
    }
}
