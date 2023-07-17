using System.Text.Json.Serialization;

namespace Cosmos.DataTransfer.AzureTableAPIExtension.Settings
{
    public class AzureTableAPIDataSinkSettings : AzureTableAPISettingsBase
    {
        /// <summary>
        /// Fields that should be skippped and not processed
        /// </summary>
        public string[] SkipFields { get; set; } = Array.Empty<string>();
        /// <summary>
        /// Fields to rename, e. g. Conflicting field name.
        /// Key is the source Name, value the new field name.
        /// </summary>
        [JsonConverter(typeof(CaseInsensitiveDictionaryConverter<string>))] 
        public Dictionary<string, string>? MapFields { get; set; }
    }
}