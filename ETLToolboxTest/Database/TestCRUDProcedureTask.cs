using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ALE.ETLToolbox;
using System.Collections.Generic;

namespace ALE.ETLToolboxTest {
    [TestClass]
    public class TestCRUDProcedureTask {
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
        public void TestCreateProcedure() {
            CRUDProcedureTask.CreateOrAlter("test.Test1", "select 1 as Test");
            Assert.IsTrue(SqlTask.ExecuteScalarAsBool("Check if proc exists", "select count(*) from sys.objects where type = 'P' and object_id = object_id('test.Test1')"));
        }

        [TestMethod]
        public void TestAlterProcedure() {
            CRUDProcedureTask.CreateOrAlter("test.Test3", "select 1 as Test");
            Assert.IsTrue(SqlTask.ExecuteScalarAsBool("Check if proc exists", "select count(*) from sys.objects where type = 'P' and object_id = object_id('test.Test3') and create_date = modify_date"));
            CRUDProcedureTask.CreateOrAlter("test.Test3", "select 5 as Test");
            Assert.IsTrue(SqlTask.ExecuteScalarAsBool("Check if proc exists", "select count(*) from sys.objects where type = 'P' and object_id = object_id('test.Test3') and create_date <> modify_date"));
        }

        [TestMethod]
        public void TestCreatProcedureWithParameter() {
            List<ProcedureParameter> pars = new List<ProcedureParameter>() {
                new ProcedureParameter("Par1", "varchar(10)"),
                new ProcedureParameter("Par2", "int", "7"),
            };
            CRUDProcedureTask.CreateOrAlter("test.Test2", "select 1 as Test",pars);
            Assert.IsTrue(SqlTask.ExecuteScalarAsBool("Check if proc exists", "select count(*) from sys.objects where type = 'P' and object_id = object_id('test.Test2')"));
            Assert.AreEqual(SqlTask.ExecuteScalar<int>("Check if parameter exists"
                , "select count(*) from sys.parameters where object_id = object_id('test.Test2')"), 2);
        }

        [TestMethod]
        public void TestLogging() {
            CreateLogTablesTask.CreateLog();
            CRUDProcedureTask.CreateOrAlter("test.Test4", "select 1 as Test");
            Assert.AreEqual(4, new SqlTask("Find log entry", "select count(*) from etl.Log where TaskType='CRUDPROC' group by TaskHash") { DisableLogging = true }.ExecuteScalar<int>());
        }

    }
}
