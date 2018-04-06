using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ALE.ETLToolbox;

namespace ALE.ETLToolboxTest
{
    [TestClass]
    public class TestAddFileGroupTask
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
        public void TestAddFileGroup() {
            string fgName = TestHelper.RandomString(10) + "_FG";
            Assert.AreEqual(0,SqlTask.ExecuteScalar<int>("FileGroup", $"select count(*) from sys.filegroups where name = '{fgName}'"));
            AddFileGroupTask.AddFileGroup(fgName, DBNameParameter, "2048KB", "2048KB", false);
            Assert.AreEqual(1,SqlTask.ExecuteScalar<int>("FileGroup", $"select count(*) from sys.filegroups where name = '{fgName}'"));
            Assert.AreEqual(1,SqlTask.ExecuteScalar<int>("FileGroup", $"select count(*) from sys.sysfiles  where name = '{fgName}'"));

        }

        [TestMethod]
        public void TestAddDefaultFileGroup() {
            string fgName = TestHelper.RandomString(10) + "_FG";
            Assert.AreEqual(0,SqlTask.ExecuteScalar<int>("FileGroup", $"select count(*) from sys.filegroups where name = '{fgName}' and is_default = 1"));
            AddFileGroupTask.AddFileGroup(fgName, DBNameParameter, "5MB", "5MB", true);
            Assert.AreEqual(1,SqlTask.ExecuteScalar<int>("FileGroup", $"select count(*) from sys.filegroups where name = '{fgName}' and is_default = 1"));
            Assert.AreEqual(1,SqlTask.ExecuteScalar<int>("FileGroup", $"select count(*) from sys.sysfiles  where name = '{fgName}'"));

        }

    }
}
