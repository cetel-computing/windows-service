using WindowsService.Config;
using WindowsService.Models;
using Dapper;
using MySqlConnector;
using Newtonsoft.Json;

namespace WindowsService
{
    /// <summary>
    ///     Consumes SysInfo results
    /// </summary>
    public class SysInfoConsumer : SimpleConsumer
    {
        public SysInfoConsumer(ResultsConsumerConfig config) : base("System Information", config)
        {
            // create the logger
            CreateLogger();
        }

        protected override string[] GetRoutingKeys() => [RcConfig.RabbitMQ.SysInfoRoutingKey];

        /// <summary>
        ///     Process result - for this consumer this means inserting the json host data representation into sql
        /// </summary>
        /// <param name="resultMetadata"></param>
        /// <param name="tag"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public override bool ProcessResult(ResultMetadata resultMetadata, string tag, CancellationToken cancellationToken)
        {
            //ensure that the result file path can be constructed
            if (resultMetadata.Path == null)
            {
                return false;
            }

            // read the result file json
            var pathToFile = $"results/{resultMetadata.Path}";
            var httpResp = _resultClient.GetAsync(pathToFile).Result;

            if (httpResp.IsSuccessStatusCode)
            {
                using (var r = new StreamReader(httpResp.Content.ReadAsStreamAsync().Result))
                {
                    var sysInfoRaw = r.ReadToEnd();
                    if (sysInfoRaw.Length == 0 || sysInfoRaw[0] == '\0')
                    {
                        Logger.Warning($"Sys Info file is empty or contains null chars. File was read from: '{pathToFile}'.");
                        return true;
                    }

                    //var sysInfo = JsonConvert.DeserializeObject<SystemInfo>(sysInfoRaw);
                    //if (sysInfo == null)
                    //{
                    //    Logger.Warning($"Sys Info JSON resulted in a null object! File was read from: '{pathToFile}'.");
                    //    return true;
                    //}

                    // ---------------- INSERT INTO SQL DB -----------------------------------
                    int dataResult = 0;

                    using (var conn = new MySqlConnection(_sqlConnection))
                    {
                        conn.Open();

                        var host = "";

                        var sql = "insert into " + _sqlDb + ".hosts() " +
                            "values() " +
                            "on duplicate key " +
                            "update ";
                        dataResult = conn.Execute(sql, host);

                    }

                    // log result
                    if (dataResult > 0)
                    {
                        Logger.Information($"Successfully processed Sys Info for host {resultMetadata.ResultId}");

                        return true;
                    }
                }
            }

            Logger.Error($"Failed to process Sys Info for host {resultMetadata.ResultId}");

            return false;
        }
    }
}
