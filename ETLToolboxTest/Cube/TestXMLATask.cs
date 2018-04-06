using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ALE.ETLToolbox;

namespace ALE.ETLToolboxTest {
    [TestClass]
    public class TestXMLATask {
        public TestContext TestContext { get; set; }

        [ClassInitialize]
        public static void TestInit(TestContext testContext) {
            string connectionString = testContext.Properties["connectionString"].ToString();
            ControlFlow.CurrentAdomdConnection = new AdomdConnectionManager(new ConnectionString(connectionString).GetConnectionWithoutCatalog());
        }


        [TestMethod]
        public void TestCreateDelete() {
            string dbName = TestContext.Properties["dbName"].ToString();
            try {
                XmlaTask.ExecuteNonQuery("Drop cube", TestHelper.DeleteCubeXMLA(dbName));
            }
            catch { }
            XmlaTask.ExecuteNonQuery("Create cube", TestHelper.CreateCubeXMLA(dbName));
            XmlaTask.ExecuteNonQuery("Delete cube", TestHelper.DeleteCubeXMLA(dbName));
        }


    }
}
