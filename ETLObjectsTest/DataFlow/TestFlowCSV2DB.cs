using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ETLObjects;
using System.Collections.Generic;
using System.Linq;
using System.Data;

namespace ETLObjectsTest {
    [TestClass]
    public class TestFlowCSV2DB {
        public TestContext TestContext { get; set; }
        public string ConnectionStringParameter => TestContext?.Properties["connectionString"].ToString();
        public string DBNameParameter => TestContext?.Properties["dbName"].ToString();

        [ClassInitialize]
        public static void TestInit(TestContext testContext) {
            TestHelper.RecreateDatabase(testContext);
            ControlFlow.CurrentDbConnection = new SqlConnectionManager(new ConnectionString(testContext.Properties["connectionString"].ToString()));
            CreateSchemaTask.Create("test");            
        }

        TableColumn keyCol => new TableColumn("Key", "int", allowNulls: false, isPrimaryKey: true) { IsIdentity = true };
        TableColumn col1 => new TableColumn("Col1", "nvarchar(100)", allowNulls: false);
        TableColumn col2 => new TableColumn("Col2", "nvarchar(50)", allowNulls: true);
        TableColumn col3 => new TableColumn("Col3", "int", allowNulls: true);

        public string[] RowTransformation1(string[] row)
        {
            return row;
        }

        public string[] RowTransformation2(string[] row)
        {
            return row;
        }

        public string[] CloneTransformation1(string[] row)
        {
            return row;
        }

        public string[][] RowTransformationMany(string[] row)
        {
            string[][] manyrows = new string[2][];

            manyrows[0] = row;

            string[] row_copy = new string[row.Length];
            row.CopyTo(row_copy, 0);
            row_copy[0] = "CopyOf" + row_copy[0];

            manyrows[1] = row_copy;

            return manyrows;
        }
        //TODO never used - remove later
        //public InMemoryTable BatchTransformation(string[][] batch)
        //{
        //    InMemoryTable table = new InMemoryTable();
        //    table.HasIdentityColumn = true;
        //    table.Columns.Add(new InMemoryColumn(col1));
        //    table.Columns.Add(new InMemoryColumn(col2));
        //    table.Columns.Add(new InMemoryColumn(col3));

        //    foreach (string[] row in batch)
        //        table.Rows.Add(row);
        //    return table;

        //}

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

            string destTable1 = "test.Staging1";
            CreateTableTask.Create(destTable1, new List<TableColumn>() {keyCol, col1, col2, col3});

            DBDestination<string[]> destination1 = new DBDestination<string[]>();
            destination1.TableName_Target = destTable1;
            destination1.FieldCount = 4;
            destination1.ObjectMappingMethod = WriterAdapter.Fill;
            destination1.Connection = ControlFlow.CurrentDbConnection;

            string destTable2 = "test.Staging2";
            CreateTableTask.Create(destTable2, new List<TableColumn>() { keyCol, col1, col2, col3 });

            DBDestination<string[]> destination2 = new DBDestination<string[]>();
            destination2.TableName_Target = destTable2;
            destination2.FieldCount = 4;
            destination2.ObjectMappingMethod = WriterAdapter.Fill;
            destination2.Connection = ControlFlow.CurrentDbConnection;

            CSVSource<string[]> CSVSource = 
                new CSVSource<string[]>("DataFlow/InputData.csv");

            Graph g = new Graph();

            g.GetVertex(0, CSVSource);
            g.GetVertex(1, new RowTransformation<string[]>(RowTransformation1));
            g.GetVertex(11, new RowTransformation<string[]>(RowTransformation2));
            g.GetVertex(10, new Broadcast<string[]>(CloneTransformation1));
            g.GetVertex(100, destination1);
            g.GetVertex(110, destination2);

            g.AddEdge(0, 1); // connect 0 to 1
            g.AddEdge(1, 10); // connect 1 to 10
            g.AddEdge(10, 100);
            g.AddEdge(10, 11);
            g.AddEdge(11, 110);

            TestHelper.VisualizeGraph(g);

            
            //DataFlowTask<string[]>.Execute("Test dataflow task", CSVSource, destination1, 3, RowTransformation);

            DataFlowTask<string[]>.Execute("Test dataflow task", 1000, 1, g);

            Assert.AreEqual(4, SqlTask.ExecuteScalar<int>("Check staging table1", string.Format("select count(*) from {0}", destTable1)));
            Assert.AreEqual(4, SqlTask.ExecuteScalar<int>("Check staging table2", string.Format("select count(*) from {0}", destTable2)));                        
        }




        
       
    }

}
