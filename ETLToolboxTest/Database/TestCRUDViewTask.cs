using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ALE.ETLToolbox;
using System.Collections.Generic;

namespace ALE.ETLToolboxTest {
    [TestClass]
    public class TestCRUDViewTask {
        public TestContext TestContext { get; set; }
        public string ConnectionStringParameter => TestContext?.Properties["connectionString"].ToString();
        public string DBNameParameter => TestContext?.Properties["dbName"].ToString();

        [ClassInitialize]
        public static void TestInit(TestContext testContext) {
            TestHelper.RecreateDatabase(testContext);
            ControlFlow.CurrentDbConnection = new SqlConnectionManager(new ConnectionString(testContext.Properties["connectionString"].ToString()));
            CreateSchemaTask.Create("test");
        }
      
        [TestMethod]
        public void TestCreateView() {
            CRUDViewTask.CreateOrAlter("test.View1", "select 1 as Test");
            Assert.IsTrue(SqlTask.ExecuteScalarAsBool("Check if view exists", "select count(*) from sys.objects where type = 'V' and object_id = object_id('test.View1')"));
        }

        [TestMethod]
        public void TestAlterView() {
            CRUDViewTask.CreateOrAlter("test.View2", "select 1 as Test");
            Assert.IsTrue(SqlTask.ExecuteScalarAsBool("Check if view exists", "select count(*) from sys.objects where type = 'V' and object_id = object_id('test.View2') and create_date = modify_date"));
            CRUDViewTask.CreateOrAlter("test.View2", "select 5 as Test");
            Assert.IsTrue(SqlTask.ExecuteScalarAsBool("Check if view exists", "select count(*) from sys.objects where type = 'V' and object_id = object_id('test.View2') and create_date <> modify_date"));
        }

        [TestMethod]
        public void TestLogging() {
            CreateLogTablesTask.CreateLog();
            CRUDViewTask.CreateOrAlter("test.View3", "select 1 as Test");
            Assert.AreEqual(4, new SqlTask("Find log entry", "select count(*) from etl.Log where TaskType='CRUDVIEW' group by TaskHash") { DisableLogging = true }.ExecuteScalar<int>());
        }



    }
}
