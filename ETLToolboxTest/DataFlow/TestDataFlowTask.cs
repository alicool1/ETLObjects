using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ALE.ETLToolbox;
using System.Collections.Generic;
using System.Linq;

namespace ALE.ETLToolboxTest {
    [TestClass]
    public class TestDataFlowTask {
        public TestContext TestContext { get; set; }
        public string ConnectionStringParameter => TestContext?.Properties["connectionString"].ToString();
        public string DBNameParameter => TestContext?.Properties["dbName"].ToString();

        [ClassInitialize]
        public static void TestInit(TestContext testContext) {
            TestHelper.RecreateDatabase(testContext);
            ControlFlow.CurrentDbConnection = new SqlConnectionManager(new ConnectionString(testContext.Properties["connectionString"].ToString()));
            CreateSchemaTask.Create("test");            
        }

        TableColumn keyCol => new TableColumn("Key", "int", allowNulls: false, isPrimaryKey: true) { IsIdentity = true };
        TableColumn col1 => new TableColumn("Col1", "nvarchar(100)", allowNulls: false);
        TableColumn col2 => new TableColumn("Col2", "nvarchar(50)", allowNulls: true);
        TableColumn col3 => new TableColumn("Col3", "int", allowNulls: true);

        [TestMethod]
        public void TestSimpleDataflow() {            
            CreateTableTask.Create("test.Staging", new List<TableColumn>() {keyCol, col1, col2, col3});
            DataFlowTask.Execute("Test dataflow task", "DataFlow/InputData.csv", "test.Staging", 3, RowTransformation, BatchTransformation);
            Assert.AreEqual(4, SqlTask.ExecuteScalar<int>("Check staging table", "select count(*) from test.Staging"));                        
        }

        public string[] RowTransformation(string[] row) {
            return row;
        }

        public InMemoryTable BatchTransformation(string[][] batch) {
            InMemoryTable table = new InMemoryTable();
            table.HasIdentityColumn = true;
            table.Columns.Add(new InMemoryColumn(col1));
            table.Columns.Add(new InMemoryColumn(col2));
            table.Columns.Add(new InMemoryColumn(col3));

            foreach (string[] row in batch)
                table.Rows.Add(row);
            return table;
        }
    }

}
