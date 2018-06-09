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
    public class TestFlowDB2DB
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

        public Datensatz RowTransformationDB2(Datensatz row)
        {
            row.F3 = row.F3 * -1;
            return row;
        }
        TableColumn Ziel_F0 => new TableColumn("F0", "int", isIdentity : true, isPrimaryKey: true, allowNulls : false);
        TableColumn Ziel_F1 => new TableColumn("F1", "int", allowNulls: true);
        TableColumn Ziel_F2 => new TableColumn("F2", "int", allowNulls: true);
        TableColumn Ziel_F3 => new TableColumn("F3", "int", allowNulls: true);

        [TestMethod]
        public void TestDataflowDbToDb()
        {
            string destTable = "test.Staging3";
            CreateTableTask.Create(destTable, new List<TableColumn>() { Ziel_F0, Ziel_F1, Ziel_F2, Ziel_F3 });

            DBSource<Datensatz> DBSource = new DBSource<Datensatz>(
                ".", "ETLToolbox"
                , "SELECT 0 as F1"
                + " UNION ALL SELECT 4 as F1"
                + " UNION ALL SELECT -3 as F1"
                + " UNION ALL SELECT -2 as F1"
                );
            DBSource.DataMappingMethod = ReaderAdapter.Read;

            DBDestination<Datensatz> destination = new DBDestination<Datensatz>();
            destination.TableName_Target = destTable;
            destination.FieldCount = 4;
            destination.ObjectMappingMethod = WriterAdapter.Fill;
            destination.Connection = ControlFlow.CurrentDbConnection;

            
            Graph g = new Graph();
            
            g.GetVertex(0, DBSource );
            g.GetVertex(1, new RowTransformation<Datensatz>(RowTransformationDB));
            g.GetVertex(2, new RowTransformation<Datensatz>(RowTransformationDB2));
            g.GetVertex(3, destination );

            g.AddEdge(0, 1); // connect 0 to 1
            g.AddEdge(1, 2); // connect 1 to 2
            g.AddEdge(2, 3); // connect 2 to 3

            //TestHelper.VisualizeGraph(g);

            //DataFlowTask<Datensatz>.Execute("Test dataflow task", DBSource, Ziel_Schreibe, 10000,1, RowTransformationDB);

            DataFlowTask<Datensatz>.Execute("Test dataflow task", 10000, 1, g);

            Assert.AreEqual(4, SqlTask.ExecuteScalar<int>("Check staging table", string.Format("select count(*) from {0}", destTable)));
        }

        

    }
}
