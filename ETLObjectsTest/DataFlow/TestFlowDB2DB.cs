using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ETLObjects;
using System.Collections.Generic;
using System.Linq;
using System.Data;
using System.Data.Common;

namespace ETLObjectsTest
{
    public partial class ETLObjectsTest
    {
        public class Datensatz_DbToDb
        {
            public int F1;
            public int F3;
        }

        public class ReaderAdapter_DbToDb
        {
            public static Datensatz_DbToDb Read(IDataRecord record)
            {
                var Datensatz = new Datensatz_DbToDb();
                Datensatz.F1 = record.GetInt32(0);
                return Datensatz;
            }
        }

        public class WriterAdapter_DbToDb
        {
            public static object[] Fill(Datensatz_DbToDb Datensatz)
            {
                object[] record = new object[4];
                record[1] = Datensatz.F1;
                record[3] = Datensatz.F3;
                return record;
            }
        }

        public Datensatz_DbToDb RowTransformationDB(Datensatz_DbToDb row)
        {
            row.F3 = row.F1 * -1;
            return row;
        }

        public Datensatz_DbToDb RowTransformationDB2(Datensatz_DbToDb row)
        {
            row.F3 = row.F3 * -1;
            return row;
        }
        TableColumn Ziel_F0 => new TableColumn("F0", SqlDbType.Int, false, true, true);
        TableColumn Ziel_F1 => new TableColumn("F1", SqlDbType.Int, true);
        TableColumn Ziel_F2 => new TableColumn("F2", SqlDbType.Int, true);
        TableColumn Ziel_F3 => new TableColumn("F3", SqlDbType.Int, true);

        [TestMethod]
        public void TestDataflowDbToDb()
        {
            using (TestDb)
            {
                string destSchema = "test";
                string destTable = "Staging3";
                string destObject = $"[{destSchema}].[{destTable}]";
                new DropAndCreateTableTask(TestDb.SqlConnection).Execute(destSchema, destTable, new List<TableColumn>() { Ziel_F0, Ziel_F1, Ziel_F2, Ziel_F3 });

                SqlSource<Datensatz_DbToDb> DBSource = new SqlSource<Datensatz_DbToDb>(TestDb.getNewSqlConnection()
                    , "SELECT 0 as F1"
                    + " UNION ALL SELECT 4 as F1"
                    + " UNION ALL SELECT -3 as F1"
                    + " UNION ALL SELECT -2 as F1"
                    );
                DBSource.DataMappingMethod = ReaderAdapter_DbToDb.Read;

                SqlDestination<Datensatz_DbToDb> destination = new SqlDestination<Datensatz_DbToDb>();
                destination.ObjectName = destObject;
                destination.FieldCount = 4;
                destination.ObjectMappingMethod = WriterAdapter_DbToDb.Fill;
                destination.SqlConnection = TestDb.SqlConnection;


                Graph g = new Graph();

                g.GetVertex(0, DBSource);
                g.GetVertex(1, new RowTransformation<Datensatz_DbToDb>(RowTransformationDB));
                g.GetVertex(2, new RowTransformation<Datensatz_DbToDb>(RowTransformationDB2));
                g.GetVertex(3, destination);

                g.AddEdge(0, 1); // connect 0 to 1
                g.AddEdge(1, 2); // connect 1 to 2
                g.AddEdge(2, 3); // connect 2 to 3



                DataFlowTask<Datensatz_DbToDb>.Execute("Test dataflow task", 10000, 1, g);

                //TestHelper.VisualizeGraph(g);

                Assert.AreEqual(4, new ExecuteSQLTask(TestDb.SqlConnection).ExecuteScalar(string.Format("select count(*) from {0}", destObject)));
            }
        }

        

    }
}
