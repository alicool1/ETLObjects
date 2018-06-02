using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ETLObjects;
using System.Collections.Generic;
using System.Linq;
using System.Data;
using System.Data.Common;

namespace ETLObjectsTest.DataFlow
{
    [TestClass]
    public class TestDataFlowTask2
    {
        public TestContext TestContext { get; set; }
        public string ConnectionStringParameter => TestContext?.Properties["connectionString"].ToString();
        public string DBNameParameter => TestContext?.Properties["dbName"].ToString();

        [ClassInitialize]
        public static void TestInit(TestContext testContext)
        {
            TestHelper.RecreateDatabase(testContext);
            ControlFlow.CurrentDbConnection = new SqlConnectionManager(new ConnectionString(testContext.Properties["connectionString"].ToString()));
            CreateSchemaTask.Create("test");
        }


        public class Datensatz
        {
            public int F1;
            public int F3;
        }

        public class ReaderAdapter
        {
            public static Datensatz Read(IDataRecord record)
            {
                var Datensatz = new Datensatz();
                Datensatz.F1 = record.GetInt32(0);
                return Datensatz;
            }
        }

        public class WriterAdapter
        {
            public static object[] Fill(Datensatz Datensatz)
            {
                object[] record = new object[4];
                record[1] = Datensatz.F1;
                record[3] = Datensatz.F3;
                return record;
            }
        }

        public Datensatz RowTransformationDB(Datensatz row)
        {
            row.F3 = row.F1 * -1;
            return row;
        }
        TableColumn Ziel_F0 => new TableColumn("F0", "int", isIdentity : true, isPrimaryKey: true, allowNulls : false);
        TableColumn Ziel_F1 => new TableColumn("F1", "int", allowNulls: true);
        TableColumn Ziel_F2 => new TableColumn("F2", "int", allowNulls: true);
        TableColumn Ziel_F3 => new TableColumn("F3", "int", allowNulls: true);

        [TestMethod]
        public void TestDataflowDbToDb()
        {
            string ZielTabelle = "test.Staging3";
            CreateTableTask.Create(ZielTabelle, new List<TableColumn>() { Ziel_F0, Ziel_F1, Ziel_F2, Ziel_F3 });

            DBSource<Datensatz> DBSource = new DBSource<Datensatz>(
                ".", "ETLToolbox"
                , "SELECT 0 as F1"
                + " UNION ALL SELECT 4 as F1"
                + " UNION ALL SELECT -3 as F1"
                + " UNION ALL SELECT -2 as F1"
                );
            DBSource.DataMappingMethod = ReaderAdapter.Read;

            DBDestination<Datensatz> Ziel_Schreibe = new DBDestination<Datensatz>();
            Ziel_Schreibe.TableName_Target = ZielTabelle;
            Ziel_Schreibe.FieldCount = 4;
            Ziel_Schreibe.ObjectMappingMethod = WriterAdapter.Fill;
            Ziel_Schreibe.Connection = ControlFlow.CurrentDbConnection;

            DataFlowTask<Datensatz>.Execute("Test dataflow task", DBSource, Ziel_Schreibe, 10000,1, RowTransformationDB);
            Assert.AreEqual(4, SqlTask.ExecuteScalar<int>("Check staging table", string.Format("select count(*) from {0}", ZielTabelle)));
        }



    }
}
