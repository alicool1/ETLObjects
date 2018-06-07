using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ETLObjects;

namespace ETLObjectsTest {
    [TestClass]
    public class TestDropCubeTask {
        public TestContext TestContext { get; set; }

        [ClassInitialize]
        public static void TestInit(TestContext testContext) {
            TestHelper.RecreateCube(testContext);
            string connectionString = testContext.Properties["connectionString"].ToString();
            ControlFlow.CurrentASConnection = new ASConnectionManager(new ConnectionString(connectionString));
            ControlFlow.CurrentDbConnection = new SqlConnectionManager(new ConnectionString(connectionString));
        }

        [TestMethod]
        public void TestDropCube() {
            DropCubeTask.Execute("Drop cube");
        }

        [TestMethod]
        public void TestLogging() {            
            TestHelper.RecreateDatabase(TestContext);            
            CreateLogTablesTask.CreateLog();            
            DropCubeTask.Execute("Drop cube");            
            Assert.AreEqual(2, new SqlTask("Find log entry", "select count(*) from etl.Log where TaskType='DROPCUBE' group by TaskHash") { DisableLogging = true }.ExecuteScalar<int>());
        }
        

    }
}
