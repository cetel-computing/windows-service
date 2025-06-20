using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace WindowsService
{
    public static class Program
    {
        public static async Task Main()
        {
            var hostBuilder =
                Host.CreateDefaultBuilder()
                    .UseWindowsService()
                    .ConfigureServices((_, collection) =>
                    {
                        collection.AddHostedService<ResultsService>();
                    });

            await hostBuilder.Build().RunAsync();
        }
    }
}
