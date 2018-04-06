using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ALE.ETLToolbox;
using System.Collections.Generic;

namespace ALE.ETLToolboxTest {
    [TestClass]
    public class TestCreateIndexTask {
        public TestContext TestContext { get; set; }
        public string ConnectionStringParameter => TestContext?.Properties["connectionString"].ToString();
        public string DBNameParameter => TestContext?.Properties["dbName"].ToString();

        [ClassInitialize]
        public static void TestInit(TestContext testContext) {
            TestHelper.RecreateDatabase(testContext);
            ControlFlow.CurrentDbConnection = new SqlConnectionManager(new ConnectionString(testContext.Properties["connectionString"].ToString()));
            CreateSchemaTask.Create("test");
            SqlTask.ExecuteNonQuery("Create test table", $@"create table test.Table1 ( key1 int null, key2 int not null)");
            SqlTask.ExecuteNonQuery("Create test table", $@"create table test.Table2 ( key1 int null, key2 int not null, value1 datetime null, value2 varchar(100) not null )");
        }
      
        [TestMethod]
        public void TestCreateIndex() {
            string indexName = "ix_" + TestHelper.RandomString(5);
            CreateIndexTask.Create(indexName, "test.Table1",new List<string>() { "key1", "key2" } );
            Assert.IsTrue(SqlTask.ExecuteScalarAsBool("Check if index exists", $"select count(*) from sys.indexes where name = '{indexName}'"));
        }

        [TestMethod]
        public void TestReCreateIndex() {
            string indexName = "ix_" + TestHelper.RandomString(5);
            CreateIndexTask.Create(indexName, "test.Table1", new List<string>() { "key1", "key2" });
            CreateIndexTask.Create(indexName, "test.Table1", new List<string>() { "key1", "key2" });
            Assert.IsTrue(SqlTask.ExecuteScalarAsBool("Check if index exists", $"select count(*) from sys.indexes where name = '{indexName}'"));
        }

        [TestMethod]
        public void TestCreateIndexWithInclude() {
            string indexName = "ix_" + TestHelper.RandomString(5);
            CreateIndexTask.Create(indexName, "test.Table2", new List<string>() { "key1","key2" }, new List<string>() { "value1", "value2"});
            Assert.IsTrue(SqlTask.ExecuteScalarAsBool("Check if index exists", $"select count(*) from sys.indexes where name = '{indexName}'"));
        }

        [TestMethod]
        public void TestLogging() {
            CreateLogTablesTask.CreateLog();
            CreateIndexTask.Create("ix_" + TestHelper.RandomString(5), "test.Table1", new List<string>() { "key1", "key2" });
            Assert.AreEqual(2, new SqlTask("Find log entry", "select count(*) from etl.Log where TaskType='CREATEINDEX' group by TaskHash") { DisableLogging = true }.ExecuteScalar<int>());
        }


    }
}
