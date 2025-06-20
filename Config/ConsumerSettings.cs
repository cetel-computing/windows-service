namespace WindowsService.Config
{
    public class ConsumerSettings
    {
        public ushort Qos { get; set; }
        public string Exchange { get; set; }
        public string Queue { get; set; }
        public string Uri { get; set; }
        public bool Persistent { get; set; }
        public string[] RoutingKeys { get; set; }
    }
}
