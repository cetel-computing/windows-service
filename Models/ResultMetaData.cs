
using Newtonsoft.Json;

namespace WindowsService.Models
{
    public class ResultMetadata
    {
        /// <summary>
        ///     The filename of the received results file
        /// </summary>
        public string Path { get; set; }
       
        /// <summary>
        ///     Any metadata produced
        /// </summary>
        public string Metadata { get; set; }

        /// <summary>
        ///     The unique id of the result
        /// </summary>
        public string ResultId { get; set; }

        public Dictionary<string, object> ParseMetadata()
        {
            if (Metadata == null)
                return new Dictionary<string, object>();

            return JsonConvert.DeserializeObject<Dictionary<string, object>>(Metadata)
                ?? new Dictionary<string, object>();
        }
    }
}
