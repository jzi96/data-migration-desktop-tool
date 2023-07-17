using System.ComponentModel.Composition;
using System.Runtime.CompilerServices;
using Azure;
using Azure.Data.Tables;
using Cosmos.DataTransfer.AzureTableAPIExtension.Data;
using Cosmos.DataTransfer.AzureTableAPIExtension.Settings;
using Cosmos.DataTransfer.Interfaces;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace Cosmos.DataTransfer.AzureTableAPIExtension
{
    [Export(typeof(IDataSourceExtension))]
    public class AzureTableAPIDataSourceExtension : IDataSourceExtensionWithSettings
    {
        public string DisplayName => "AzureTableAPI";

        public async IAsyncEnumerable<IDataItem> ReadAsync(IConfiguration config, ILogger logger, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            var settings = config.Get<AzureTableAPIDataSourceSettings>();
            settings.Validate();

            var tco = new TableClientOptions(TableClientOptions.ServiceVersion.V2020_12_06);
            tco.Retry.Mode = Azure.Core.RetryMode.Exponential;
            tco.Retry.NetworkTimeout = TimeSpan.FromMilliseconds(500);
            tco.Retry.Delay = TimeSpan.FromMilliseconds(100);
            tco.Retry.MaxDelay = TimeSpan.FromSeconds(20);
            tco.Retry.MaxRetries = 10;
            var serviceClient = new TableServiceClient(settings.ConnectionString, tco);
            var tableClient = serviceClient.GetTableClient(settings.Table);

            //Pageable<TableEntity> queryResultsFilter = tableClient.Query<TableEntity>(filter: $"PartitionKey eq '{partitionKey}'");
            AsyncPageable<TableEntity> queryResults;
            logger.LogDebug("Begin reading data  ... {query}", settings.QueryFilter);
            if (!string.IsNullOrWhiteSpace(settings.QueryFilter)) {
                queryResults = tableClient.QueryAsync<TableEntity>(filter: settings.QueryFilter);
            } else {
                queryResults = tableClient.QueryAsync<TableEntity>();
            }

            var enumerator = queryResults.GetAsyncEnumerator();
            while (await enumerator.MoveNextAsync())
            {
                yield return new AzureTableAPIDataItem(enumerator.Current, settings.PartitionKeyFieldName, settings.RowKeyFieldName);
            }
            //do
            //{
            //    yield return new AzureTableAPIDataItem(enumerator.Current, settings.PartitionKeyFieldName, settings.RowKeyFieldName);
            //} while (await enumerator.MoveNextAsync());
        }

        public IEnumerable<IDataExtensionSettings> GetSettings()
        {
            yield return new AzureTableAPIDataSourceSettings();
        }
    }
}