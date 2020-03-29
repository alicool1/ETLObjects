using Microsoft.VisualStudio.TestTools.UnitTesting;
using ETLObjects;
using System.Data;

namespace ETLObjectsTest
{
    public partial class ETLObjectsTest
    {

        public class Datensatz_MySql_to_List
        {
            public int Tag;
            public int Monat;
        }

        public class ReaderAdapter_MySql_to_List
        {
            public static Datensatz_MySql_to_List Read(IDataRecord record)
            {
                var Datensatz = new Datensatz_MySql_to_List();
                Datensatz.Tag = record.GetInt32(0);
                Datensatz.Monat = record.GetInt32(1);
                return Datensatz;
            }
        }


        public Datensatz_MySql_to_List RowTransformationDB(Datensatz_MySql_to_List row)
        {
            row.Tag = row.Tag * -1;
            return row;
        }

        public Datensatz_MySql_to_List RowTransformationDB2(Datensatz_MySql_to_List row)
        {
            row.Monat = row.Monat * -1;
            return row;
        }

        [TestMethod]
        public void TestDataflow_MySql_to_List()
        {
            string ServerName = "";
            if (string.IsNullOrEmpty(ServerName)) return;
            MySqlConnectionManager MyTestDb = new MySqlConnectionManager(ServerName, "TestDB", "", "");
            

            using (MyTestDb)
            {
               
                MySqlSource<Datensatz_MySql_to_List> MySqlDBSource = new MySqlSource<Datensatz_MySql_to_List>(MyTestDb.getNewMySqlConnection()
                    , "SELECT Tag, Monat FROM test"
                    );
                MySqlDBSource.DataMappingMethod = ReaderAdapter_MySql_to_List.Read;

                ListDestination<Datensatz> destination = new ListDestination<Datensatz>();

                Graph g = new Graph();

                g.GetVertex(0, MySqlDBSource);
                g.GetVertex(1, new RowTransformation<Datensatz_MySql_to_List>(RowTransformationDB));
                g.GetVertex(2, new RowTransformation<Datensatz_MySql_to_List>(RowTransformationDB2));
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
