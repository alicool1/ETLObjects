using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ETLObjects;
using System.Collections.Generic;
using System.Linq;
using System.Data;

namespace ETLObjectsTest {
    [TestClass]
    public class TestDataFlowTask {
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

        public string[] RowTransformation(string[] row)
        {
            return row;
        }

        public InMemoryTable BatchTransformation(string[][] batch)
        {
            InMemoryTable table = new InMemoryTable();
            table.HasIdentityColumn = true;
            table.Columns.Add(new InMemoryColumn(col1));
            table.Columns.Add(new InMemoryColumn(col2));
            table.Columns.Add(new InMemoryColumn(col3));

            foreach (string[] row in batch)
                table.Rows.Add(row);
            return table;

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
        public void TestSimpleDataflow() {

            string ZielTabelle = "test.Staging2";
            CreateTableTask.Create(ZielTabelle, new List<TableColumn>() {keyCol, col1, col2, col3});


            DBDestination<string[]> Ziel_Schreibe = new DBDestination<string[]>();
            Ziel_Schreibe.TableName_Target = ZielTabelle;
            Ziel_Schreibe.FieldCount = 4;
            Ziel_Schreibe.ObjectMappingMethod = WriterAdapter.Fill;
            Ziel_Schreibe.Connection = ControlFlow.CurrentDbConnection;

            CSVSource<string[]> CSVSource = 
                new CSVSource<string[]>("DataFlow/InputData.csv");

            Graph g = new Graph();

            g.getVertex(0, CSVSource);
            g.getVertex(1, new RowTransformFunction<string[]>(RowTransformation));
            g.getVertex(2, Ziel_Schreibe);

            g.addEdge(0, 1); // connect 0 to 1
            g.addEdge(1, 2); // connect 1 to 2


            TestHelper.VisualizeGraph(g);

            //DataFlowTask<string[]>.Execute("Test dataflow task", CSVSource, Ziel_Schreibe, 3, RowTransformation, BatchTransformation);

            DataFlowTask<string[]>.Execute("Test dataflow task", 10000, 1, g);


            Assert.AreEqual(4, SqlTask.ExecuteScalar<int>("Check staging table", string.Format("select count(*) from {0}", ZielTabelle)));                        
        }




        
       
    }

}
