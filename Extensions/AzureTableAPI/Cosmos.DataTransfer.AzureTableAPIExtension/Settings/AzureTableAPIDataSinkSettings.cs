namespace Cosmos.DataTransfer.AzureTableAPIExtension.Settings
{
    public class AzureTableAPIDataSinkSettings : AzureTableAPISettingsBase
    {
        public int MaxParallelWrites { get; set; } = 1000;
    }
}