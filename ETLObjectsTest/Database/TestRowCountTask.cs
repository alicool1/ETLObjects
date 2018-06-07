using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ETLObjects;
using System.Collections.Generic;

namespace ETLObjectsTest {
    [TestClass]
    public class TestRowCountTask {
        public TestContext TestContext { get; set; }
        public string ConnectionStringParameter => TestContext?.Properties["connectionString"].ToString();
        public string DBNameParameter => TestContext?.Properties["dbName"].ToString();

        [ClassInitialize]
        public static void TestInit(TestContext testContext) {
            TestHelper.RecreateDatabase(testContext);
            ControlFlow.CurrentDbConnection = new SqlConnectionManager(new ConnectionString(testContext.Properties["connectionString"].ToString()));
            CreateSchemaTask.Create("test");
            SqlTask.ExecuteNonQuery("Create test data table",$@"
create table test.RC ( value int null )
insert into test.RC select * from (values (1), (2), (3)) AS MyTable(v)");
        }
      
        [TestMethod]
        public void TestCount() {

            Assert.AreEqual(3, RowCountTask.Count("test.RC"));
        }

        [TestMethod]
        public void TestCountWithCondition() {
            Assert.AreEqual(1, RowCountTask.Count("test.RC", "value = 2"));
        }

        [TestMethod]
        public void TestLogging() {
            CreateLogTablesTask.CreateLog();
            RowCountTask.Count("test.RC");
            Assert.AreEqual(2, new SqlTask("Find log entry", "select count(*) from etl.Log where TaskType='ROWCOUNT' group by TaskHash") { DisableLogging = true }.ExecuteScalar<int>());
            RowCountTask.Count("test.RC", "value = 2");
            Assert.AreEqual(2, new SqlTask("Find log entry", "select count(*) from etl.Log where TaskType='ROWCOUNT' and Message like '%with condition%' group by TaskHash") { DisableLogging = true }.ExecuteScalar<int>());            
        }
    }
}
