using Azure.Data.Tables;
using Cosmos.DataTransfer.AzureTableAPIExtension.Settings;
using Cosmos.DataTransfer.Interfaces;
using System;

namespace Cosmos.DataTransfer.AzureTableAPIExtension.Data
{
    public static class AzureTableAPIDataItemExtensions
    {
        public static TableEntity ToTableEntity(this IDataItem item, AzureTableAPIDataSinkSettings settings)
        {
            var entity = new TableEntity();
            string? PartitionKeyFieldName = settings.PartitionKeyFieldName;
            string? RowKeyFieldName= settings.RowKeyFieldName;

            var partitionKeyFieldNameToUse = "PartitionKey";
            if (!string.IsNullOrWhiteSpace(PartitionKeyFieldName))
            {
                partitionKeyFieldNameToUse = PartitionKeyFieldName;
            }

            var rowKeyFieldNameToUse = "RowKey";
            if (!string.IsNullOrWhiteSpace(RowKeyFieldName))
            {
                rowKeyFieldNameToUse = RowKeyFieldName;
            }

            foreach (var key in item.GetFieldNames())
            {
                if (Contains(settings.SkipFields, key))
                    continue;

                if (key.Equals(partitionKeyFieldNameToUse, StringComparison.InvariantCultureIgnoreCase))
                {
                    var partitionKey = item.GetValue(key)?.ToString();
                    entity.PartitionKey = partitionKey;
                }
                else if (key.Equals(rowKeyFieldNameToUse, StringComparison.InvariantCultureIgnoreCase))
                {
                    var rowKey = item.GetValue(key)?.ToString();
                    entity.RowKey = rowKey;
                }
                else
                {
                    if(settings.MapFields is not null && settings.MapFields.TryGetValue(key, out var newKey))
                        entity.Add(newKey, item.GetValue(key));
                    else
                        entity.Add(key, item.GetValue(key));
                }
            }

            return entity;
        }
        private static bool Contains(string[] fields, string searchField)
        {
            var fs = fields.AsSpan();
            for (int i = 0; i < fs.Length; i++)
            {
                var cf = fs[i];
                if(cf.Equals(searchField, StringComparison.OrdinalIgnoreCase)) { return true; }
            }
            return false;
        }
    }
}
