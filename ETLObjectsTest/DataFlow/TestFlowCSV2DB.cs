using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ETLObjects;
using System.Collections.Generic;
using System.Linq;
using System.Data;

namespace ETLObjectsTest
{
    [TestClass]
    public class TestFlowCSV2DB {
        public TestContext TestContext { get; set; }
        
        static SqlConnectionManager TestDb = null;

        [ClassInitialize]
        public static void TestInit(TestContext testContext) {

            string ServerName = testContext.Properties["ServerName"].ToString();
            string InitialCatalog = testContext.Properties["InitialCatalog"].ToString();
            TestDb = new SqlConnectionManager(ServerName, InitialCatalog);
            new CreateSchemaTask(TestDb.SqlConnection).Create("test");        
        }

        

        private int counter_RowTransformation1 = 0;
        public string[] RowTransformation1(string[] row)
        {
            counter_RowTransformation1++;
            row[2] = (Int32.Parse(row[2]) + 100).ToString();
            return row;
        }

        private int counter_RowTransformation2 = 0;
        public string[] RowTransformation2(string[] row)
        {
            counter_RowTransformation2++;
            string[] row_copy = new string[row.Length];
            row.CopyTo(row_copy, 0);
            row_copy[2] = (Int32.Parse(row_copy[2]) - 10).ToString();
            return row_copy;
        }

        private int counter_RowTransformation3 = 0;
        public string[] RowTransformation3(string[] row)
        {
            counter_RowTransformation3++;
            string[] row_copy = new string[row.Length];
            row.CopyTo(row_copy, 0);
            row_copy[2] = (Int32.Parse(row_copy[2]) - 20).ToString();
            return row_copy;
        }

        public string[] CloneTransformation1(string[] row)
        {
            return row;
        }

        private int counter_RowTransformationMany = 0;
        public string[][] RowTransformationMany(string[] row)
        {
            int many = 2;
            string[][] manyrows = new string[many][];

            manyrows[0] = row;

            string[] row_copy = new string[row.Length];
            row.CopyTo(row_copy, 0);
            row_copy[0] = "CopyOf" + row_copy[0];

            manyrows[1] = row_copy;

            counter_RowTransformationMany = counter_RowTransformationMany + many;
            return manyrows;
        }
       
        public class WriterAdapter
        {
            public static object[] Fill(string[] Datensatz)
            {
                object[] record = new object[4];
                record[1] = Datensatz[0];
                record[2] = Datensatz[1];
                record[3] = Datensatz[2];
                return record;
            }
        }


        [TestMethod]
        public void TestSampleDataflow() {
            using (TestDb)
            {
                TableColumn keyCol = new TableColumn("Key", SqlDbType.Int, false, true, true);
                TableColumn col1 = new TableColumn("Col1", SqlDbType.NVarChar, 100, false);
                TableColumn col2 = new TableColumn("Col2", SqlDbType.NVarChar, 50, true);
                TableColumn col3 = new TableColumn("Col3", SqlDbType.Int, false);


                string destSchema = "test";
                string destTable1 = "Staging1";
                string destObject1 = $"[{destSchema}].[{destTable1}]";
                new DropAndCreateTableTask(TestDb.SqlConnection).Execute(destSchema, destTable1, new List<TableColumn>() { keyCol, col1, col2, col3 });

                DBDestination<string[]> destination1 = new DBDestination<string[]>();
                destination1.ObjectName = destObject1;
                destination1.FieldCount = 4;
                destination1.ObjectMappingMethod = WriterAdapter.Fill;
                destination1.SqlConnection = TestDb.getNewSqlConnection();

                string destTable2 = "Staging2";
                string destObject2 = $"[{destSchema}].[{destTable2}]";
                new DropAndCreateTableTask(TestDb.SqlConnection).Execute(destSchema, destTable2, new List<TableColumn>() { keyCol, col1, col2, col3 });



                DBDestination<string[]> destination2 = new DBDestination<string[]>();
                destination2.ObjectName = destObject2;
                destination2.FieldCount = 4;
                destination2.ObjectMappingMethod = WriterAdapter.Fill;
                destination2.SqlConnection = TestDb.getNewSqlConnection();

                CSVSource<string[]> CSVSource =
                    new CSVSource<string[]>("DataFlow/InputData.csv");

                Graph g = new Graph();

                g.GetVertex(0, CSVSource);
                g.GetVertex(1, new RowTransformation<string[]>(RowTransformation1));
                g.GetVertex(11, new RowTransformation<string[]>(RowTransformation2));
                g.GetVertex(10, new BroadCast<string[]>(CloneTransformation1));
                g.GetVertex(12, new RowTransformationMany<string[]>(RowTransformationMany));
                g.GetVertex(20, new RowTransformation<string[]>(RowTransformation3));
                g.GetVertex(100, destination1);
                g.GetVertex(110, destination2);

                Edge e1 = g.AddEdge(0, 1); // connect 0 to 1
                Edge e2 = g.AddEdge(1, 10); // connect 1 to 10
                Edge e3 = g.AddEdge(10, 20);
                Edge e4 = g.AddEdge(20, 100);
                Edge e5 = g.AddEdge(10, 11);
                Edge e6 = g.AddEdge(11, 12);
                Edge e7 = g.AddEdge(12, 110);


                DataFlowTask<string[]>.Execute("Test dataflow task", 1000, 1, g);

                e2.cost = counter_RowTransformation1;
                e6.cost = counter_RowTransformation2;
                e4.cost = counter_RowTransformation3;

                e7.cost = counter_RowTransformationMany;

                TestHelper.VisualizeGraph(g);

                Assert.AreEqual(4, new ExecuteSQLTask(TestDb.SqlConnection).ExecuteScalar(string.Format("select count(*) from {0}", destObject1)));
                Assert.AreEqual(8, new ExecuteSQLTask(TestDb.SqlConnection).ExecuteScalar(string.Format("select count(*) from {0}", destObject2)));

            }
        }




        
       
    }

}
