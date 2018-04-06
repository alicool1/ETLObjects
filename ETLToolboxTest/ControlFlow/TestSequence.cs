using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ALE.ETLToolbox;
using System.Collections.Generic;

namespace ALE.ETLToolboxTest {
    [TestClass]
    public class TestSequence {
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
        public void TestSequence1() {
            Sequence.Execute("Test sequence 1", Action1);
            Assert.IsTrue(Action1Executed);
        }

        public void Action1() {
            Action1Executed = true;
        }

        public bool Action2Executed { get; set; }

        [TestMethod]
        public void TestSequence2() {
            string test = "Test";
            Sequence<object>.Execute("Test sequence 2", Action2, test);
            Assert.IsTrue(Action2Executed);
        }

        public void Action2(object parent) {
            Action2Executed = true;
            Assert.AreEqual("Test", parent);            
        }

        [TestMethod]
        public void TestLogging() {
            CreateLogTablesTask.CreateLog();
            Sequence.Execute("Test sequence 3", Action1);
            Assert.AreEqual(2, new SqlTask("Find log entry", "select count(*) from etl.Log where TaskType='SEQUENCE' group by TaskHash") { DisableLogging = true }.ExecuteScalar<int>());
        }



    }
}
