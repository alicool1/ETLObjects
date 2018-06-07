using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ETLObjects;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ETLObjectsTest {
    [TestClass]
    public class TestLogging {
        public TestContext TestContext { get; set; }
        public string ConnectionStringParameter => TestContext?.Properties["connectionString"].ToString();
        public string DBNameParameter => TestContext?.Properties["dbName"].ToString();

        [ClassInitialize]
        public static void TestInit(TestContext testContext) {
            ControlFlow.STAGE = "SETUP";
            ControlFlow.CurrentDbConnection = new SqlConnectionManager(new ConnectionString(testContext.Properties["connectionString"].ToString()));
        }

        [TestInitialize]
        public void BeforeTests() {
            TestHelper.RecreateDatabase(TestContext);
            CreateLogTablesTask.CreateLog();
            Assert.IsFalse(new SqlTask("Check if logging was disabled for table creation", "select count(*) from etl.Log") { DisableLogging = true }.ExecuteScalarAsBool());
            Assert.IsTrue(new SqlTask("Check etl.Log table", $"select count(*) from sys.tables where type = 'U' and name = 'Log' and schema_id('etl') = schema_id") { DisableLogging = true }.ExecuteScalarAsBool());
            Assert.IsTrue(new SqlTask("Check etl.LoadProcess table", $"select count(*) from sys.tables where type = 'U' and name = 'LoadProcess' and schema_id('etl') = schema_id") { DisableLogging = true }.ExecuteScalarAsBool());
        }


        [TestMethod]
        public void TestErrorLogging() {
            LogTask.Error("Error");
            LogTask.Warn("Warn");
            LogTask.Info("Info");
            Assert.AreEqual(3, SqlTask.ExecuteScalar<int>("Check if default log works", "select count(*) from etl.Log where Message in ('Error','Warn','Info')"));
        }

        [TestMethod]
        public void TestRemoveLogTablesTask() {
            RemoveLogTablesTask.Remove();
            CreateLogTablesTask.CreateLog();
            RemoveLogTablesTask.Remove();
            Assert.IsTrue(SqlTask.ExecuteScalarAsBool("Check if tables are deleted", "select case when object_id('etl.LoadProcess')  is null then 1 else 0 end"));
            Assert.IsTrue(SqlTask.ExecuteScalarAsBool("Check if tables are deleted", "select case when object_id('etl.Log')  is null then 1 else 0 end"));
        }

        [TestMethod]
        public void TestLoadProcessKeyIfRestarted() {
            StartLoadProcessTask.Start("Test process 6");
            int? processKey1 = ControlFlow.CurrentLoadProcess.LoadProcessKey;
            SqlTask.ExecuteNonQuery("Test Task", "Select 1 as test");
            Assert.AreEqual(2, new SqlTask("Check if load process key is set", $"select count(*) from etl.Log where Message='Test Task' and LoadProcessKey = {processKey1}") { DisableLogging = true }.ExecuteScalar<int>());

            StartLoadProcessTask.Start("Test process 7");
            int? processKey2 = ControlFlow.CurrentLoadProcess.LoadProcessKey;
            Assert.AreNotEqual(processKey1, processKey2);
            SqlTask.ExecuteNonQuery("Test Task", "Select 1 as test");
            Assert.AreEqual(2, new SqlTask("Check if load process key is set", $"select count(*) from etl.Log where Message='Test Task' and LoadProcessKey = {processKey2}") { DisableLogging = true }.ExecuteScalar<int>());
        }

        [TestMethod]
        public void TestLogCleanup() {
            LogTask.Error("Error");
            LogTask.Warn("Warn");
            LogTask.Info("Info");
            CleanUpLogTask.CleanUp(0);
            Assert.AreEqual(0, new SqlTask("Check if log table is empty", $"select count(*) from etl.Log ") { DisableLogging = true }.ExecuteScalar<int>());
        }

        [TestMethod]
        public void TestLoadProcessKeyForLogTask() {
            StartLoadProcessTask.Start("Test process 8");
            int? processKey1 = ControlFlow.CurrentLoadProcess.LoadProcessKey;
            LogTask.Error("Test1");
            LogTask.Warn("Test2");
            LogTask.Info("Test3");
            Assert.AreEqual(3, new SqlTask("Check if load process key is set", $"select count(*) from etl.Log where Message like 'Test%' and LoadProcessKey = {processKey1}") { DisableLogging = true }.ExecuteScalar<int>());
        }
    }

}
