using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ALE.ETLToolbox;

namespace ALE.ETLToolboxTest
{
    [TestClass]
    public class TestCreateDeleteDatabaseTask
    {
        public TestContext TestContext { get; set; }

        [ClassInitialize]
        public static void TestInit(TestContext testContext)
        {
            string connectionString = testContext.Properties["connectionString"].ToString();
            ControlFlow.CurrentDbConnection = new SqlConnectionManager(new ConnectionString(connectionString).GetMasterConnection());
        }

        [TestMethod]
        public void TestDropDB() {
            string dbName = TestContext.Properties["dbName"].ToString();
            var sqlTask = new SqlTask("Get assert data", $"select cast(db_id('{dbName}') as int)");

            CreateDatabaseTask.Create(dbName);

            Assert.IsTrue(sqlTask.ExecuteScalarAsBool());

            DropDatabaseTask.Delete(dbName);

            Assert.IsFalse(sqlTask.ExecuteScalarAsBool());
        }


        [TestMethod]
        public void TestCreateWithAllParameters()
        {            
            string dbName = TestContext.Properties["dbName"].ToString();
            var sqlTask = new SqlTask("Get assert data", $"select cast(db_id('{dbName}') as int)");

            DropDatabaseTask.Delete(dbName);
            
            Assert.IsFalse(sqlTask.ExecuteScalarAsBool());

            CreateDatabaseTask.Create(dbName, RecoveryModel.Simple, "Latin1_General_CS_AS");            
                        
            Assert.IsTrue(sqlTask.ExecuteScalarAsBool());
            
        }

    
    }
}
