using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ETLObjects;
using System.Collections.Generic;
using System.IO;
using System.Text.RegularExpressions;
using Newtonsoft.Json.Linq;

namespace ETLObjectsTest {
    [TestClass]
    public class TestGetLoadProcessAsJSONTask {
        public TestContext TestContext { get; set; }
        public string ConnectionStringParameter => TestContext?.Properties["connectionString"].ToString();
        public string DBNameParameter => TestContext?.Properties["dbName"].ToString();

        [ClassInitialize]
        public static void TestInit(TestContext testContext) {
            TestHelper.RecreateDatabase(testContext);
            ControlFlow.CurrentDbConnection = new SqlConnectionManager(new ConnectionString(testContext.Properties["connectionString"].ToString()));
            CreateLogTablesTask.CreateLog();
        }


        [TestMethod]
        public void TestGet1LoadProcessAsJSON() {
            RunProcess1();
            string response = GetLoadProcessAsJSONTask.GetJSON();
            JArray json = JArray.Parse(response);

            Assert.AreEqual("Process 1", (string)json[0]["processName"]);
            Assert.AreEqual(false, (bool)json[0]["isRunning"]);
            Assert.AreEqual(true, (bool)json[0]["wasSuccessful"]);
            Assert.AreEqual(false, (bool)json[0]["wasAborted"]);
            Assert.AreEqual(true, (bool)json[0]["isFinished"]);
            Assert.AreEqual(false, (bool)json[0]["isTransferCompleted"]);
            Assert.AreEqual("Start", (string)json[0]["startMessage"]);
            Assert.AreEqual("End", (string)json[0]["endMessage"]);
        }


        private void RunProcess1() {
            StartLoadProcessTask.Start("Process 1","Start");
            SqlTask.ExecuteNonQuery($"Just some sql", "Select 1 as test");
            EndLoadProcessTask.End("End");
        }


    }
}
