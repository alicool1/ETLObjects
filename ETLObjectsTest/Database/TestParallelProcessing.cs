using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ETLObjects;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace ETLObjectsTest {
    [TestClass]
    public class TestParallelProcessing {
        public TestContext TestContext { get; set; }
        public string ConnectionStringParameter => TestContext?.Properties["connectionString"].ToString();
        public string DBNameParameter => TestContext?.Properties["dbName"].ToString();

        [ClassInitialize]
        public static void TestInit(TestContext testContext) {
            TestHelper.RecreateDatabase(testContext);
            
        }
      
        [TestMethod]
        public void TestSqLTaskInParallel() {
            ControlFlow.CurrentDbConnection = new SqlConnectionManager(new ConnectionString(TestContext.Properties["connectionString"].ToString()));
            List<int> array = new List<int>() { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
            Parallel.ForEach(array, new ParallelOptions { MaxDegreeOfParallelism = 2 },
                curNr => SqlTask.ExecuteNonQuery($"Test statement {curNr}", $"select 1")
             );
            

        }
    }
}
