using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ETLObjects;
using System.Collections.Generic;

namespace ETLObjectsTest {
    [TestClass]
    public class TestCreateSchemaTask {
        public TestContext TestContext { get; set; }
        public string ConnectionStringParameter => TestContext?.Properties["connectionString"].ToString();
        public string DBNameParameter => TestContext?.Properties["dbName"].ToString();

        [ClassInitialize]
        public static void TestInit(TestContext testContext) {
            TestHelper.RecreateDatabase(testContext);
            ControlFlow.CurrentDbConnection = new SqlConnectionManager(new ConnectionString(testContext.Properties["connectionString"].ToString()));
        }
      
        [TestMethod]
        public void TestCreateSchema() {
            string schemaName = "s" + TestHelper.RandomString(9);
            CreateSchemaTask.Create(schemaName);
            Assert.IsTrue(SqlTask.ExecuteScalarAsBool("Check if schema exists", $"select count(*) from sys.schemas where schema_name(schema_id) = '{schemaName}'"));
         
        }

        [TestMethod]
        public void TestLogging() {
            CreateLogTablesTask.CreateLog();
            CreateSchemaTask.Create("s" + TestHelper.RandomString(9));
            Assert.AreEqual(2, new SqlTask("Find log entry", "select count(*) from etl.Log where TaskType='CREATESCHEMA' group by TaskHash") { DisableLogging = true }.ExecuteScalar<int>());
        }


    }
}
