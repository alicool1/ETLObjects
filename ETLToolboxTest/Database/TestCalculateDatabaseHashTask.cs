using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ETLObjects;
using System.Collections.Generic;
using System.Linq;

namespace ETLObjectsTest {
    [TestClass]
    public class TestCalculateDatabaseTask {
        public TestContext TestContext { get; set; }
        public string ConnectionStringParameter => TestContext?.Properties["connectionString"].ToString();
        public string DBNameParameter => TestContext?.Properties["dbName"].ToString();

        [ClassInitialize]
        public static void TestInit(TestContext testContext) {
            TestHelper.RecreateDatabase(testContext);
            ControlFlow.CurrentDbConnection = new SqlConnectionManager(new ConnectionString(testContext.Properties["connectionString"].ToString()));
            CreateSchemaTask.Create("test");            
        }

        [TestMethod]
        public void TestHashCalculationForOneTable() {
            List<TableColumn> columns = new List<TableColumn>() { new TableColumn("value", "int") };
            CreateTableTask.Create("test.Table1", columns);

            string hash = CalculateDatabaseHashTask.Calculate(new List<string>() { "test" });
            string hashAgain = CalculateDatabaseHashTask.Calculate(new List<string>() { "test" });

            Assert.AreEqual(hash, hashAgain);
            Assert.AreEqual("A35318F3AE62DD0BA0607BB24F2103CFE77661B3", hash);

        }        

    }
}
