using System.ComponentModel.Composition;
using System.Diagnostics;
using Azure.Data.Tables;
using Cosmos.DataTransfer.AzureTableAPIExtension.Data;
using Cosmos.DataTransfer.AzureTableAPIExtension.Settings;
using Cosmos.DataTransfer.Interfaces;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace Cosmos.DataTransfer.AzureTableAPIExtension
{
    [Export(typeof(IDataSinkExtension))]
    public class AzureTableAPIDataSinkExtension : IDataSinkExtensionWithSettings
    {
        public string DisplayName => "AzureTableAPI";

        public async Task WriteAsync(IAsyncEnumerable<IDataItem> dataItems, IConfiguration config, IDataSourceExtension dataSource, ILogger logger, CancellationToken cancellationToken = default)
        {
            //var readingSource = dataSource as AzureTableAPIDataSourceExtension;
            var settings = config.Get<AzureTableAPIDataSinkSettings>();
            settings.Validate();
            var tco = new TableClientOptions(TableClientOptions.ServiceVersion.V2020_12_06);
            tco.Retry.Mode = Azure.Core.RetryMode.Exponential;
            tco.Retry.NetworkTimeout = TimeSpan.FromMilliseconds(500);
            tco.Retry.Delay = TimeSpan.FromSeconds(5);
            tco.Retry.MaxDelay = TimeSpan.FromSeconds(60);
            tco.Retry.MaxRetries = 50;
            var serviceClient = new TableServiceClient(settings.ConnectionString, tco);
            var tableClient = serviceClient.GetTableClient(settings.Table);

            await tableClient.CreateIfNotExistsAsync(cancellationToken);
            var maxParallelTask = settings.MaxParallelWrites;
            var createTasks = new List<Task>(maxParallelTask);
            var importSw = Stopwatch.StartNew();
            int chunk = 0;
            SemaphoreSlim sm = new SemaphoreSlim(maxParallelTask, maxParallelTask);
            try
            {
                await foreach (var item in dataItems.WithCancellation(cancellationToken))
                {
                    logger.LogDebug("Begin processing write ... {SemaphoreCount}", sm.CurrentCount);
                    await sm.WaitAsync();
                    var entity = item.ToTableEntity(settings.PartitionKeyFieldName, settings.RowKeyFieldName);
                    createTasks.Add(tableClient.UpsertEntityAsync(entity, cancellationToken: cancellationToken));

                    if (sm.CurrentCount == 0)
                    {
                        var ch = Interlocked.Increment(ref chunk);
                        logger.LogDebug("Cleaning up max write items for batch={chunk}", ch);
                        await Task.WhenAll(createTasks);
                        createTasks.Clear();
                        sm.Release(maxParallelTask);
                    }
                }

                await Task.WhenAll(createTasks);
            }
            finally
            {
                sm.Dispose();
                importSw.Stop();
                logger.LogInformation("The import (write) of all items (chunks={chunks}) took {elapsed}", chunk, importSw.Elapsed);
            }
        }

        public IEnumerable<IDataExtensionSettings> GetSettings()
        {
            yield return new AzureTableAPIDataSinkSettings();
        }
    }
}
