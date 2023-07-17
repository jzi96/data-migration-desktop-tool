using Cosmos.DataTransfer.AzureTableAPIExtension.Data;

namespace Cosmos.DataTransfer.AzureTableAPIExtension.UnitTests.Data
{
    [TestClass]
    public class AzureTableAPIDataItemExtensionsTests
    {
        [TestMethod]
        public void AzureTableAPIDataItem_GetString_01()
        {
            var dataitem = new MockDataItem(new Dictionary<string, object>()
            {
                { "Name", "Chris" }
            });

            var entity = dataitem.ToTableEntity(new Settings.AzureTableAPIDataSinkSettings());
            Assert.IsTrue(entity.ContainsKey("Name"));
            Assert.AreEqual("Chris", entity.GetString("Name"));
        }

        [TestMethod]
        public void AzureTableAPIDataItem_GetInt32_01()
        {
            var dataitem = new MockDataItem(new Dictionary<string, object>()
            {
                { "Number", 123 }
            });

            var entity = dataitem.ToTableEntity(new Settings.AzureTableAPIDataSinkSettings());
            Assert.IsTrue(entity.ContainsKey("Number"));
            Assert.AreEqual(123, entity.GetInt32("Number"));
        }

        [TestMethod]
        public void AzureTableAPIDataItem_PartitionKey_Int_01()
        {
            var dataitem = new MockDataItem(new Dictionary<string, object>()
            {
                { "PartitionKey", 123 }
            });

            var entity = dataitem.ToTableEntity(new Settings.AzureTableAPIDataSinkSettings());
            Assert.AreEqual(1, entity.Keys.Count());
            Assert.AreEqual("123", entity.PartitionKey);
        }

        [TestMethod]
        public void AzureTableAPIDataItem_PartitionKey_Int_02()
        {
            var dataitem = new MockDataItem(new Dictionary<string, object>()
            {
                { "MyID", 123 }
            });

            var entity = dataitem.ToTableEntity(new Settings.AzureTableAPIDataSinkSettings() { PartitionKeyFieldName = "MyID" });
            Assert.AreEqual(1, entity.Keys.Count());
            Assert.AreEqual("123", entity.PartitionKey);
        }

        [TestMethod]
        public void AzureTableAPIDataItem_PartitionKey_String_01()
        {
            var dataitem = new MockDataItem(new Dictionary<string, object>()
            {
                { "PartitionKey", "WI" }
            });

            var entity = dataitem.ToTableEntity(new Settings.AzureTableAPIDataSinkSettings());
            Assert.AreEqual(1, entity.Keys.Count());
            Assert.AreEqual("WI", entity.PartitionKey);
        }

        [TestMethod]
        public void AzureTableAPIDataItem_PartitionKey_String_02()
        {
            var dataitem = new MockDataItem(new Dictionary<string, object>()
            {
                { "MyVal", "WI" }
            });

            var entity = dataitem.ToTableEntity(new Settings.AzureTableAPIDataSinkSettings() { PartitionKeyFieldName = "MyVal" });
            Assert.AreEqual(1, entity.Keys.Count());
            Assert.AreEqual("WI", entity.PartitionKey);
        }

        [TestMethod]
        public void AzureTableAPIDataItem_RowKey_Int_01()
        {
            var dataitem = new MockDataItem(new Dictionary<string, object>()
            {
                { "RowKey", 123 }
            });

            var entity = dataitem.ToTableEntity(new Settings.AzureTableAPIDataSinkSettings());
            Assert.AreEqual(1, entity.Keys.Count());
            Assert.AreEqual("123", entity.RowKey);
        }

        [TestMethod]
        public void AzureTableAPIDataItem_RowKey_Int_02()
        {
            var dataitem = new MockDataItem(new Dictionary<string, object>()
            {
                { "MyKey", 123 }
            });

            var entity = dataitem.ToTableEntity(new Settings.AzureTableAPIDataSinkSettings() { RowKeyFieldName = "MyKey" });
            Assert.AreEqual(1, entity.Keys.Count());
            Assert.AreEqual("123", entity.RowKey);
        }

        [TestMethod]
        public void AzureTableAPIDataItem_RowKey_String_01()
        {
            var dataitem = new MockDataItem(new Dictionary<string, object>()
            {
                { "RowKey", "WI" }
            });

            var entity = dataitem.ToTableEntity(new Settings.AzureTableAPIDataSinkSettings());
            Assert.AreEqual(1, entity.Keys.Count());
            Assert.AreEqual("WI", entity.RowKey);
        }

        [TestMethod]
        public void AzureTableAPIDataItem_RowKey_String_02()
        {
            var dataitem = new MockDataItem(new Dictionary<string, object>()
            {
                { "MyVal", "WI" }
            });

            var entity = dataitem.ToTableEntity(new Settings.AzureTableAPIDataSinkSettings() { RowKeyFieldName = "MyVal" });
            Assert.AreEqual(1, entity.Keys.Count());
            Assert.AreEqual("WI", entity.RowKey);
        }

        [TestMethod]
        public void AzureTableAPIDataItem_PartitionKey_RowKey_01()
        {
            var dataitem = new MockDataItem(new Dictionary<string, object>()
            {
                { "PartitionKey", "WI" },
                { "RowKey", 123 }
            });

            var entity = dataitem.ToTableEntity(new Settings.AzureTableAPIDataSinkSettings() { PartitionKeyFieldName = String.Empty, RowKeyFieldName = String.Empty });
            Assert.AreEqual(2, entity.Keys.Count());
            Assert.AreEqual("WI", entity.PartitionKey);
            Assert.AreEqual("123", entity.RowKey);
        }

        [TestMethod]
        public void AzureTableAPIDataItem_PartitionKey_RowKey_02()
        {
            var dataitem = new MockDataItem(new Dictionary<string, object>()
            {
                { "MyVal", "Tailspin" },
                { "ID", 456 }
            });

            var entity = dataitem.ToTableEntity(new Settings.AzureTableAPIDataSinkSettings() { PartitionKeyFieldName = "MyVal", RowKeyFieldName = "ID" });
            Assert.AreEqual(2, entity.Keys.Count());
            Assert.AreEqual("Tailspin", entity.PartitionKey);
            Assert.AreEqual("456", entity.RowKey);
        }

        [TestMethod]
        public void AzureTableAPIDataItem_SkipField_01()
        {
            var dataitem = new MockDataItem(new Dictionary<string, object>()
            {
                { "RemoveId", "WIRem" },
                { "Id", "WI" }
            });

            Settings.AzureTableAPIDataSinkSettings settings = new Settings.AzureTableAPIDataSinkSettings
            {
                PartitionKeyFieldName = "Id",
                SkipFields = new string[] { "RemoveId" }
            };
            var entity = dataitem.ToTableEntity(settings);
            Assert.AreEqual(1, entity.Keys.Count());
            Assert.AreEqual("WI", entity.PartitionKey);
        }
        [TestMethod]
        public void AzureTableAPIDataItem_SkipField_03()
        {
            var dataitem = new MockDataItem(new Dictionary<string, object>()
            {
                { "RemoveId", "WIRem" },
                { "Id", "WI" }
            });

            Settings.AzureTableAPIDataSinkSettings settings = new Settings.AzureTableAPIDataSinkSettings
            {
                PartitionKeyFieldName = "Id",
                SkipFields = new string[] { "removeId" }
            };
            var entity = dataitem.ToTableEntity(settings);
            Assert.AreEqual(1, entity.Keys.Count());
            Assert.AreEqual("WI", entity.PartitionKey);
        }
        [TestMethod]
        public void AzureTableAPIDataItem_SkipField_02()
        {
            var dataitem = new MockDataItem(new Dictionary<string, object>()
            {
                { "RemoveId", "WIRem" },
                { "Id", "WI" },
                { "AnyField", "CH" }
            });

            Settings.AzureTableAPIDataSinkSettings settings = new Settings.AzureTableAPIDataSinkSettings
            {
                PartitionKeyFieldName = "Id",
                SkipFields = new string[] { "RemoveId" }
            };
            var entity = dataitem.ToTableEntity(settings);
            Assert.AreEqual(2, entity.Keys.Count());
            Assert.AreEqual("WI", entity.PartitionKey);
        }
        [TestMethod]
        public void AzureTableAPIDataItem_MapField_01()
        {
            var dataitem = new MockDataItem(new Dictionary<string, object>()
            {
                { "RemoveId", "WIRem" },
                { "Id", "WI" }
            });

            Settings.AzureTableAPIDataSinkSettings settings = new Settings.AzureTableAPIDataSinkSettings
            {
                PartitionKeyFieldName = "Id",
                MapFields = new Dictionary<string, string>() { { "RemoveId", "NewField"} }
            };
            var entity = dataitem.ToTableEntity(settings);
            Assert.AreEqual(2, entity.Keys.Count());
            Assert.AreEqual("WI", entity.PartitionKey);
            Assert.AreEqual("WIRem", entity["NewField"]);
        }
    }
}
