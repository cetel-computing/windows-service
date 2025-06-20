namespace WindowsService.Config
{
    public class ResultsConsumerConfig
    {
        public string LogFilePath { get; set; }
        public RabbitMQ RabbitMQ { get; set; }
        public SQLConfigOptions SQLConfigOptions { get; set; }
    }

    public class RabbitMQ
    {
        public string Hostname { get; set; }
        public int Port { get; set; }
        public string Exchange { get; set; }
        public string Username { get; set; }
        public string Password { get; set; }
        public string VirtualHost { get; set; }
        public string QueuePrefix { get; set; }
        public string AlertsRoutingKey { get; set; }
        public string ServicesRoutingKey { get; set; }
        public string ResolutionsRoutingKey { get; set; }
        public string SettingsRoutingKey { get; set; }
        public string StatusRoutingKey { get; set; }
        public string SysInfoRoutingKey { get; set; }
    }

    public class SQLConfigOptions
    {
        public IEnumerable<string> Hosts { get; set; }
        public string Username { get; set; }
        public string Password { get; set; }
        public string Database { get; set; }
    }
}
