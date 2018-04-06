using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ALE.ETLToolbox;
using System.Collections.Generic;

namespace ALE.ETLToolboxTest {
    [TestClass]
    public class TestSqlTask {
        public TestContext TestContext { get; set; }
        public string ConnectionStringParameter => TestContext?.Properties["connectionString"].ToString();
        public string DBNameParameter => TestContext?.Properties["dbName"].ToString();

        [ClassInitialize]
        public static void TestInit(TestContext testContext) {
            TestHelper.RecreateDatabase(testContext);
            ControlFlow.CurrentDbConnection = new SqlConnectionManager(new ConnectionString(testContext.Properties["connectionString"].ToString()));
        }
      
        [TestMethod]
        public void TestExecuteNonQuery() {
            string propName = TestHelper.RandomString(10);
            SqlTask.ExecuteNonQuery("Test add extended property", $"exec sp_addextendedproperty @name = N'{propName}', @value = 'Test';");
            string asisCollation = SqlTask.ExecuteScalar("Get reference result", $"select value from fn_listextendedproperty('{propName}', default, default, default, default, default, default)").ToString();
            Assert.AreEqual("Test", asisCollation);
            SqlTask.ExecuteNonQuery("Drop extended property", $"exec sp_dropextendedproperty @name = N'{propName}'");

        }

        [TestMethod]
        public void TestExecuteScalar() {
            object result = SqlTask.ExecuteScalar("Test execute scalar", "select cast('Hallo Welt' as nvarchar(100)) as ScalarResult");
            Assert.AreEqual(result.ToString(), "Hallo Welt");

        }

        [TestMethod]
        public void TestExecuteScalarDatatype() {
            decimal result = (decimal)(SqlTask.ExecuteScalar("Test execute scalar with datatype", "select cast(1.343 as numeric(4,3)) as ScalarResult"));
            Assert.AreEqual(result, 1.343m);

        }

        [TestMethod]
        public void TestExecuteScalarAsBool() {
            bool result = SqlTask.ExecuteScalarAsBool("Test execute scalar as bool", "select 1 as Bool");
            Assert.IsTrue(result);
        }        

        [TestMethod]
        public void TestExecuteReaderSingleColumn() {
            List<int> asIsResult = new List<int>();
            List<int> toBeResult = new List<int>() { 1, 2, 3 };
            SqlTask.ExecuteReader("Test execute reader", "SELECT * FROM (VALUES (1),(2),(3)) MyTable(a)",
                colA => asIsResult.Add((int)colA));
            CollectionAssert.AreEqual(asIsResult, toBeResult);
        }

        public class ThreeInteger : IEquatable<ThreeInteger> {
            public int A { get; set; }
            public int B { get; set; }
            public int C { get; set; }
            public ThreeInteger() { }
            public ThreeInteger(int a, int b, int c) {
                A = a; B = b; C = c;
            }
            public bool Equals(ThreeInteger other) => other != null ? other.A == A && other.B == B && other.C == C : false;
            public override bool Equals(object obj) {
                return this.Equals((ThreeInteger)obj);
            }
        }

        [TestMethod]
        public void TestExecuteReaderMultiColumn() {
            List<ThreeInteger> asIsResult = new List<ThreeInteger>();
            List<ThreeInteger> toBeResult = new List<ThreeInteger>() { new ThreeInteger(1, 2, 3), new ThreeInteger(4, 5, 6), new ThreeInteger(7, 8, 9) };
            ThreeInteger CurColumn = new ThreeInteger();
            SqlTask.ExecuteReader("Test execute reader", "SELECT * FROM (VALUES (1, 2, 3), (4, 5, 6), (7, 8, 9)) AS MyTable(a,b,c)"
                , () => CurColumn = new ThreeInteger()
                , () => asIsResult.Add(CurColumn)
                , colA => CurColumn.A = (int)colA
                , colB => CurColumn.B = (int)colB
                , colC => CurColumn.C = (int)colC
                );
            CollectionAssert.AreEqual(asIsResult, toBeResult);
        }

        [TestMethod]
        public void TestLogging() {
            RemoveLogTablesTask.Remove();
            CreateLogTablesTask.CreateLog();
            SqlTask.ExecuteNonQuery("Test select", $"select 1 as test");
            Assert.AreEqual(2, new SqlTask("Find log entry", "select count(*) from etl.Log where TaskType='SQL' group by TaskHash") { DisableLogging = true }.ExecuteScalar<int>());
        }

        [TestMethod]
        public void TestLoggingWithoutCFConnection() {
            ControlFlow.CurrentDbConnection = null;
            var connection = new SqlConnectionManager(new ConnectionString(TestContext.Properties["connectionString"].ToString()));            
            new RemoveLogTablesTask() { ConnectionManager = connection }.Execute();
            new CreateLogTablesTask() { ConnectionManager = connection }.Execute();
            new SqlTask("Test select", $"select 1 as test") { ConnectionManager = connection }.ExecuteNonQuery();
            Assert.AreEqual(2, new SqlTask("Find log entry", "select count(*) from etl.Log where TaskType='SQL' group by TaskHash") { DisableLogging = true, ConnectionManager = connection }.ExecuteScalar<int>());
        }
    }
}
