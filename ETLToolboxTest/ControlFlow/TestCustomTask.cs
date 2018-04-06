using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ALE.ETLToolbox;
using System.Collections.Generic;

namespace ALE.ETLToolboxTest {
    [TestClass]
    public class TestCustomTask {
        public TestContext TestContext { get; set; }
        public string ConnectionStringParameter => TestContext?.Properties["connectionString"].ToString();
        public string DBNameParameter => TestContext?.Properties["dbName"].ToString();

        [ClassInitialize]
        public static void TestInit(TestContext testContext) {
            TestHelper.RecreateDatabase(testContext);
            ControlFlow.CurrentDbConnection = new SqlConnectionManager(new ConnectionString(testContext.Properties["connectionString"].ToString()));
        }
      
        public bool Action1Executed { get; set; }

        [TestMethod]
        public void TestCustomTask1() {
            CustomTask.Execute("Test custom task 1", Action1);
            Assert.IsTrue(Action1Executed);
        }

        public void Action1() {
            Action1Executed = true;
        }

        public int Action2Value { get; set; }

        [TestMethod]
        public void TestCustomTask2() {
            CustomTask.Execute("Test custom task 2", Action2, 5);
            Assert.AreEqual(5, Action2Value);
        }

        public void Action2(int param1) {
            Action2Value = param1;
        }

        public string Action3Value1 { get; set; }
        public bool Action3Value2 { get; set; }

        [TestMethod]
        public void TestCustomTask3() {
            CustomTask.Execute("Test custom task 3", Action3, "t",true);
            Assert.AreEqual("t", Action3Value1);
            Assert.IsTrue(Action3Value2);
        }

        public void Action3(string param1, bool param2) {
            Action3Value1 = param1;
            Action3Value2 = param2;
        }

        [TestMethod]
        public void TestLogging() {
            CreateLogTablesTask.CreateLog();
            CustomTask.Execute("Test custom task 4", Action1);
            Assert.AreEqual(2, new SqlTask("Find log entry", "select count(*) from etl.Log where TaskType='CUSTOM' group by TaskHash") { DisableLogging = true }.ExecuteScalar<int>());
        }

    }
}
