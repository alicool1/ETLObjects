using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ETLObjects;
using System.Collections.Generic;
using System.Linq;

namespace ETLObjectsTest {
    [TestClass]
    public class TestGetDatabaseListTask {
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
        public void TestGetDatabaseList() {
            List<string> allDatabases = GetDatabaseListTask.List();

            Assert.IsTrue(allDatabases.Count > 1);
            Assert.IsTrue(allDatabases.Any(name => name == DBNameParameter));

        }        

    }
}
