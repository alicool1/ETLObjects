using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ALE.ETLToolbox;

namespace ALE.ETLToolboxTest {
    [TestClass]
    public class TestProcessCubeTask {
        public TestContext TestContext { get; set; }

        [ClassInitialize]
        public static void TestInit(TestContext testContext) {
            TestHelper.RecreateCube(testContext);
            string connectionString = testContext.Properties["connectionString"].ToString();
            ControlFlow.CurrentASConnection = new ASConnectionManager(new ConnectionString(connectionString));
            ControlFlow.CurrentDbConnection = new SqlConnectionManager(new ConnectionString(connectionString));
        }

        [TestMethod]
        public void TestProcess() {
            ProcessCubeTask.Process("Process cube test");
        }

        [TestMethod]
        public void TestLogging() {
            TestHelper.RecreateDatabase(TestContext);
            CreateLogTablesTask.CreateLog();
            ProcessCubeTask.Process("Process cube test");
            Assert.AreEqual(2, new SqlTask("Find log entry", "select count(*) from etl.Log where TaskType='PROCESSCUBE' group by TaskHash") { DisableLogging = true }.ExecuteScalar<int>());
        }



    }
}
