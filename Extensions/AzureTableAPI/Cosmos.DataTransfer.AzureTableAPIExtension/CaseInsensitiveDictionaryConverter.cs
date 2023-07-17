using System.Text.Json;
using System.Text.Json.Serialization;

namespace Cosmos.DataTransfer.AzureTableAPIExtension
{
    public sealed class CaseInsensitiveDictionaryConverter<TValue>
    : JsonConverter<Dictionary<string, TValue>>
    {
        public override Dictionary<string, TValue> Read(
            ref Utf8JsonReader reader,
            Type typeToConvert,
            JsonSerializerOptions options)
        {
            var dic = (Dictionary<string, TValue>)JsonSerializer
                .Deserialize(ref reader, typeToConvert, options);
            if (dic is null)
                return new Dictionary<string, TValue>(StringComparer.OrdinalIgnoreCase);
            return new Dictionary<string, TValue>(dic, StringComparer.OrdinalIgnoreCase);
        }

        public override void Write(
            Utf8JsonWriter writer,
            Dictionary<string, TValue> value,
            JsonSerializerOptions options)
        {
            JsonSerializer.Serialize(
                writer, value, value.GetType(), options);
        }
    }
}
