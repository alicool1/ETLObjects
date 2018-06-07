using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ETLObjects;
using System.Collections.Generic;
using System.IO;
using System.Text.RegularExpressions;

namespace ETLObjectsTest {
    [TestClass]
    public class TestGetLogAsJSONTask {
        public TestContext TestContext { get; set; }
        public string ConnectionStringParameter => TestContext?.Properties["connectionString"].ToString();
        public string DBNameParameter => TestContext?.Properties["dbName"].ToString();

        [ClassInitialize]
        public static void TestInit(TestContext testContext) {
            ControlFlow.ClearSettings();
            TestHelper.RecreateDatabase(testContext);
            ControlFlow.CurrentDbConnection = new SqlConnectionManager(new ConnectionString(testContext.Properties["connectionString"].ToString()));
            CreateLogTablesTask.CreateLog();
        }


        [TestMethod]
        public void TestGetDemoLogAsJSON() {
            RunDemoProcess();
            string jsonresult = GetLogAsJSONTask.GetJSON();
            jsonresult = RemoveDates(jsonresult.ToLower().Trim());
            string expectedresult = RemoveDates(File.ReadAllText("Logging/demolog_tobe.json").ToLower().Trim());
            Assert.AreEqual(expectedresult, jsonresult);
        }

        private void RunDemoProcess() {
            new Sequence("Test sequence 1", RunSubSequence) { TaskType = "SUBPACKAGE" }.Execute();
            SqlTask.ExecuteNonQuery($"Sql #1", "Select 1 as test");
            LogTask.Info("Info message");
        }

        private static string RemoveDates(string jsonresult) => Regex.Replace(jsonresult, @"[0-9]+-[0-9]+-[0-9]([T]|\w)+[0-9]+:[0-9]+:[0-9]+[.][0-9]+", "");

        public void RunSubSequence() {
            Sequence.Execute("Test sub sequence 1.1", () => {
                SqlTask.ExecuteNonQuery($"Sql #2", "Select 1 as test");
                SqlTask.ExecuteNonQuery($"Sql #3", "Select 1 as test");
                LogTask.Warn("Warn message #1");
            });
            Sequence.Execute("Test sub sequence 1.2", () => {
                SqlTask.ExecuteNonQuery($"Sql #4", "Select 1 as test");
            });
            Sequence.Execute("Test sub sequence 1.3",
                () => {
                    Sequence.Execute("Test sub sequence 2.1", () => {
                        Sequence.Execute("Test sub sequence 3.1", () => {
                            SqlTask.ExecuteNonQuery($"Sql #5", "Select 1 as test");
                            SqlTask.ExecuteNonQuery($"Sql #6", "Select 1 as test");
                            LogTask.Warn("Warn message #2");
                        });
                        CustomTask.Execute($"Custom #1", () => {; });
                        SqlTask.ExecuteNonQuery($"Sql #7", "Select 1 as test");

                    });
                    Sequence.Execute("Test sub sequence 2.2", () => {
                        CustomTask.Execute($"Custom #2", () => {; });
                        SqlTask.ExecuteNonQuery($"Sql #7", "Select 1 as test");
                    });
                    Sequence.Execute("Test sub sequence 2.3", () => {
                        SqlTask.ExecuteNonQuery($"Sql #8", "Select 1 as test");
                        CustomTask.Execute($"Custom #2", () => {; });
                        Sequence.Execute("Test sub sequence 3.3", () => {
                            SqlTask.ExecuteNonQuery($"Sql #9", "Select 1 as test");
                            SqlTask.ExecuteNonQuery($"Sql #10", "Select 1 as test");
                            LogTask.Error("Error message");
                        });
                    });
                });
            CustomTask.Execute($"Custom #3", () => {; });
        }
    }
}
