using Microsoft.VisualStudio.TestTools.UnitTesting;
using ETLObjects;
using System.Data;
using MySql.Data.MySqlClient;
using System.Collections.Generic;

namespace ETLObjectsTest.DataFlow
{
    [TestClass]
    public class MySql_to_List
    {
        public TestContext TestContext { get; set; }
        static MySqlConnectionManager MyTestDb = null;

        [ClassInitialize]
        public static void TestInit(TestContext testContext)
        {
            MyTestDb = new MySqlConnectionManager("", "TestDB", "", "");
        }

        public class Datensatz
        {
            public int Tag;
            public int Monat;
        }

        public class ReaderAdapter
        {
            public static Datensatz Read(IDataRecord record)
            {
                var Datensatz = new Datensatz();
                Datensatz.Tag = record.GetInt32(0);
                Datensatz.Monat = record.GetInt32(1);
                return Datensatz;
            }
        }


        public Datensatz RowTransformationDB(Datensatz row)
        {
            row.Tag = row.Tag * -1;
            return row;
        }

        public Datensatz RowTransformationDB2(Datensatz row)
        {
            row.Monat = row.Monat * -1;
            return row;
        }

        [TestMethod]
        public void TestDataflow_MySql_to_List()
        {
            using (MyTestDb)
            {
               
                MySqlSource<Datensatz> MySqlDBSource = new MySqlSource<Datensatz>(MyTestDb.getNewMySqlConnection()
                    , "SELECT Tag, Monat FROM test"
                    );
                MySqlDBSource.DataMappingMethod = ReaderAdapter.Read;

                ListDestination<Datensatz> destination = new ListDestination<Datensatz>();

                Graph g = new Graph();

                g.GetVertex(0, MySqlDBSource);
                g.GetVertex(1, new RowTransformation<Datensatz>(RowTransformationDB));
                g.GetVertex(2, new RowTransformation<Datensatz>(RowTransformationDB2));
                g.GetVertex(3, destination);

                g.AddEdge(0, 1); // connect 0 to 1
                g.AddEdge(1, 2); // connect 1 to 2
                g.AddEdge(2, 3); // connect 2 to 3



                DataFlowTask<Datensatz>.Execute("Test dataflow task", 10000, 1, g);

                //TestHelper.VisualizeGraph(g);

                //Assert.AreEqual(4, new ExecuteSQLTask(MyTestDb.MySqlConnection).ExecuteScalar(string.Format("select count(*) from {0}", destObject)));
            }
        }

        

    }
}
