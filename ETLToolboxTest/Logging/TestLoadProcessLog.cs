using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ALE.ETLToolbox;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ALE.ETLToolboxTest {
    [TestClass]
    public class TestLoadProcessLog {
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
            Assert.IsTrue(new SqlTask("Check proc StartLoadProcess", $"select count(*) from sys.procedures  where type = 'P' and name = 'StartLoadProcess' and schema_id = schema_id('etl')") { DisableLogging = true }.ExecuteScalarAsBool());
            Assert.IsTrue(new SqlTask("Check proc EndLoadProcess", $"select count(*) from sys.procedures  where type = 'P' and name = 'EndLoadProcess' and schema_id = schema_id('etl')") { DisableLogging = true }.ExecuteScalarAsBool());
            Assert.IsTrue(new SqlTask("Check proc AbortLoadProcess", $"select count(*) from sys.procedures  where type = 'P' and name = 'AbortLoadProcess' and schema_id = schema_id('etl')") { DisableLogging = true }.ExecuteScalarAsBool());
        }

        [TestMethod]
        public void TestStartLoadProcessTask() {
            DateTime beforeTask = DateTime.Now;
            Task.Delay(10).Wait(); //Sql Server datetime is not that exact

            StartLoadProcessTask.Start("Test process 1");

            DateTime afterTask = DateTime.Now;
            Assert.IsNotNull(ControlFlow.CurrentLoadProcess);
            Assert.AreEqual("Test process 1", ControlFlow.CurrentLoadProcess.ProcessName);
            Assert.IsTrue(ControlFlow.CurrentLoadProcess.StartDate <= afterTask && ControlFlow.CurrentLoadProcess.StartDate >= beforeTask);
            Assert.IsFalse(new SqlTask("Check if logging was disabled for start process task", "select count(*) from etl.Log") { DisableLogging = true }.ExecuteScalarAsBool());
            Assert.AreEqual(1, new SqlTask("Check if load process messages are correct", $"select count(*) from etl.LoadProcess where StartMessage is null and EndMessage is null and AbortMessage is null") { DisableLogging = true }.ExecuteScalar<int>());
            Assert.AreEqual(1, new SqlTask("Check if load process entry is correct", $"select count(*) from etl.LoadProcess where IsRunning=1 and WasSuccessful=0 and WasAborted=0") { DisableLogging = true }.ExecuteScalar<int>());

        }

        [TestMethod]
        public void TestEndLoadProcessTask() {
            StartLoadProcessTask.Start("Test process 2");
            Assert.IsTrue(ControlFlow.CurrentLoadProcess.IsRunning == true);
            DateTime beforeTask = DateTime.Now;
            Task.Delay(10).Wait(); //Sql Server datetime is not that exact

            EndLoadProcessTask.End();

            DateTime afterTask = DateTime.Now;
            Assert.IsTrue(ControlFlow.CurrentLoadProcess.IsRunning == false);
            Assert.IsTrue(ControlFlow.CurrentLoadProcess.WasSuccessful == true);
            Assert.IsTrue(ControlFlow.CurrentLoadProcess.IsFinished == true);
            Assert.IsTrue(ControlFlow.CurrentLoadProcess.EndDate <= afterTask && ControlFlow.CurrentLoadProcess.EndDate >= beforeTask);
            Assert.IsFalse(new SqlTask("Check if logging was disabled for end process task", "select count(*) from etl.Log") { DisableLogging = true }.ExecuteScalarAsBool());
            Assert.AreEqual(1, new SqlTask("Check if load process entry is correct", $"select count(*) from etl.LoadProcess where IsRunning=0 and WasSuccessful=1 and WasAborted=0") { DisableLogging = true }.ExecuteScalar<int>());
            Assert.AreEqual(1, new SqlTask("Check if load process messages are correct", $"select count(*) from etl.LoadProcess where StartMessage is null and EndMessage is null and AbortMessage is null") { DisableLogging = true }.ExecuteScalar<int>());
        }

        [TestMethod]
        public void TestAbortLoadProcessTask() {
            StartLoadProcessTask.Start("Test process 3");
            Assert.IsTrue(ControlFlow.CurrentLoadProcess.IsRunning == true);
            DateTime beforeTask = DateTime.Now;
            Task.Delay(10).Wait(); //Sql Server datetime is not that exact

            AbortLoadProcessTask.Abort(ControlFlow.CurrentLoadProcess.LoadProcessKey, "AbortMessage");

            DateTime afterTask = DateTime.Now;
            Assert.IsTrue(ControlFlow.CurrentLoadProcess.IsRunning == false);
            Assert.IsTrue(ControlFlow.CurrentLoadProcess.WasAborted == true);
            Assert.IsTrue(ControlFlow.CurrentLoadProcess.EndDate <= afterTask && ControlFlow.CurrentLoadProcess.EndDate >= beforeTask);
            Assert.IsTrue(ControlFlow.CurrentLoadProcess.AbortMessage == "AbortMessage");
            Assert.IsFalse(new SqlTask("Check if logging was disabled for end process task", "select count(*) from etl.Log") { DisableLogging = true }.ExecuteScalarAsBool());
            Assert.AreEqual(1, new SqlTask("Check if load process entry is correct", $"select count(*) from etl.LoadProcess where IsRunning=0 and WasSuccessful=0 and WasAborted=1") { DisableLogging = true }.ExecuteScalar<int>());

        }

        [TestMethod]
        public void TestIsTransferCompletedForLoadProcessTask() {
            StartLoadProcessTask.Start("Test process 4");
            Assert.IsTrue(ControlFlow.CurrentLoadProcess.IsRunning == true);
            DateTime beforeTask = DateTime.Now;
            Task.Delay(10).Wait(); //Sql Server datetime is not that exact

            TransferCompletedForLoadProcessTask.Complete(ControlFlow.CurrentLoadProcess.LoadProcessKey);
            Assert.AreEqual(2, new SqlTask("Check if transfer completed was in log", "select count(*) from etl.Log where TaskType='TRANSFERCOMPLETE'") { DisableLogging = true }.ExecuteScalar<int>());
            DateTime afterTask = DateTime.Now;
            Assert.IsTrue(ControlFlow.CurrentLoadProcess.IsRunning == true);
            Assert.IsTrue(ControlFlow.CurrentLoadProcess.TransferCompletedDate <= afterTask && ControlFlow.CurrentLoadProcess.TransferCompletedDate >= beforeTask);
        }

        [TestMethod]
        public void TestLoadProcessKeyInLog() {
            StartLoadProcessTask.Start("Test process 5");
            SqlTask.ExecuteNonQuery("Test Task", "Select 1 as test");
            Assert.AreEqual(2, new SqlTask("Check if load process key is set", $"select count(*) from etl.Log where Message='Test Task' and LoadProcessKey = {ControlFlow.CurrentLoadProcess.LoadProcessKey}") { DisableLogging = true }.ExecuteScalar<int>());
        }

        [TestMethod]
        public void TestReadLastSuccessfulProcess() {
            StartLoadProcessTask.Start("Test process 8");
            Task.Delay(10).Wait(); //Sql Server datetime is not that exact
            EndLoadProcessTask.End();
            Task.Delay(10).Wait();
            StartLoadProcessTask.Start("Test process 9");
            Task.Delay(10).Wait(); //Sql Server datetime is not that exact
            EndLoadProcessTask.End();

            var lp = ReadLoadProcessTableTask.ReadWithOption(ReadOptions.ReadLastSuccessful);
            Assert.IsTrue(lp.IsFinished);
            Assert.IsTrue(lp.WasSuccessful);
            Assert.IsFalse(lp.WasAborted);
            Assert.AreEqual("Test process 9", lp.ProcessName);
            Assert.AreEqual(2, new SqlTask("Check if finished processes exists", $"select count(*) from etl.LoadProcess where IsFinished=1") { DisableLogging = true }.ExecuteScalar<int>());
            Assert.AreEqual(2, new SqlTask("Check if successful processes exists", $"select count(*) from etl.LoadProcess where WasSuccessful=1") { DisableLogging = true }.ExecuteScalar<int>());
        }

        [TestMethod]
        public void TestReadLastAbortedProcess() {
            StartLoadProcessTask.Start("Test process 10");
            Task.Delay(10).Wait(); //Sql Server datetime is not that exact
            EndLoadProcessTask.End();
            Task.Delay(10).Wait();
            StartLoadProcessTask.Start("Test process 11");
            Task.Delay(10).Wait(); //Sql Server datetime is not that exact
            AbortLoadProcessTask.Abort();
            StartLoadProcessTask.Start("Test process 12");
            Task.Delay(10).Wait(); //Sql Server datetime is not that exact
            EndLoadProcessTask.End();

            var lp = ReadLoadProcessTableTask.ReadWithOption(ReadOptions.ReadLastAborted);
            Assert.IsTrue(lp.IsFinished);
            Assert.IsTrue(lp.WasAborted);
            Assert.IsFalse(lp.WasSuccessful);
            Assert.AreEqual("Test process 11", lp.ProcessName);
            Assert.AreEqual(3, new SqlTask("Check if finished processes exists", $"select count(*) from etl.LoadProcess where IsFinished=1") { DisableLogging = true }.ExecuteScalar<int>());
            Assert.AreEqual(2, new SqlTask("Check if successful processes exists", $"select count(*) from etl.LoadProcess where WasSuccessful=1") { DisableLogging = true }.ExecuteScalar<int>());
            Assert.AreEqual(1, new SqlTask("Check if aborted processes exists", $"select count(*) from etl.LoadProcess where WasAborted=1") { DisableLogging = true }.ExecuteScalar<int>());
        }

    }

}
