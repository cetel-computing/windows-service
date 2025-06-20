namespace WindowsService.Helpers
{
    internal class OutstandingMessage
    {
        public string RoutingKey { get; }
        public byte[] Body { get; }

        public OutstandingMessage(string routingKey, byte[] body)
        {
            RoutingKey = routingKey;
            Body = body;
        }
    }
}