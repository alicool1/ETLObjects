using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ALE.ETLToolbox;

namespace ALE.ETLToolboxTest
{
    [TestClass]
    public class TestCleanUpSchemaTask
    {
        public TestContext TestContext { get; set; }
        public string ConnectionStringParameter => TestContext?.Properties["connectionString"].ToString();
        public string DBNameParameter => TestContext?.Properties["dbName"].ToString();

        [ClassInitialize]
        public static void TestInit(TestContext testContext) {
            TestHelper.RecreateDatabase(testContext);
            ControlFlow.CurrentDbConnection = new SqlConnectionManager(new ConnectionString(testContext.Properties["connectionString"].ToString()));
        }
       
        [TestMethod]
        public void TestCleanUpSchema()
        {
            string schemaName = "s"+TestHelper.RandomString(9);
            SqlTask.ExecuteNonQuery("Create schema", $"create schema {schemaName}");
            SqlTask.ExecuteNonQuery("Create schema", $"create table {schemaName}.Table1 ( Nothing int null )");
            SqlTask.ExecuteNonQuery("Create schema", $"create view {schemaName}.View1 as select * from {schemaName}.Table1");
            SqlTask.ExecuteNonQuery("Create schema", $"create procedure {schemaName}.Proc1 as select * from {schemaName}.Table1");
            var objCountSql = new SqlTask("Count object", $@"select count(*) from sys.objects obj 
 inner join sys.schemas sch  on sch.schema_id = obj.schema_id
where sch.name = '{schemaName}'");
            Assert.AreEqual(3,objCountSql.ExecuteScalar<int>());
            CleanUpSchemaTask.CleanUp(schemaName);
            Assert.AreEqual(0,objCountSql.ExecuteScalar<int>());
        }

        [TestMethod]
        public void TestCleanupETLLogTables() {            
            CreateLogTablesTask.CreateLog();            
            Assert.IsTrue(new SqlTask("Check etl.Log table", $"select count(*) from sys.tables where type = 'U' and name = 'Log' and schema_id('etl') = schema_id") { DisableLogging = true }.ExecuteScalarAsBool());
            Assert.IsTrue(new SqlTask("Check etl.LoadProcess table", $"select count(*) from sys.tables where type = 'U' and name = 'LoadProcess' and schema_id('etl') = schema_id") { DisableLogging = true }.ExecuteScalarAsBool());
            CleanUpSchemaTask.CleanUp("etl");
            Assert.IsFalse(new SqlTask("Check etl.Log table", $"select count(*) from sys.tables where type = 'U' and name = 'Log' and schema_id('etl') = schema_id") { DisableLogging = true }.ExecuteScalarAsBool());
            Assert.IsFalse(new SqlTask("Check etl.LoadProcess table", $"select count(*) from sys.tables where type = 'U' and name = 'LoadProcess' and schema_id('etl') = schema_id") { DisableLogging = true }.ExecuteScalarAsBool());
        }
    }
}
