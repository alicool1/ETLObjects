using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ALE.ETLToolbox;
using System.Collections.Generic;
using System.Linq;
using System.Data;

namespace ALE.ETLToolboxTest {
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
        [TestMethod]
        public void TestSimpleDataflow() {

            string ZielTabelle = "test.Staging2";
            CreateTableTask.Create(ZielTabelle, new List<TableColumn>() {keyCol, col1, col2, col3});

            CSVSource<string[]> CSVSource = new CSVSource<string[]>("DataFlow/InputData.csv");
            DataFlowTask<string[]>.Execute("Test dataflow task", CSVSource, ZielTabelle, 3, RowTransformation, BatchTransformation);
            Assert.AreEqual(4, SqlTask.ExecuteScalar<int>("Check staging table", string.Format("select count(*) from {0}", ZielTabelle)));                        
        }




        public class Datensatz
        {
            public int F1;
            public int F2;
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
                object[] record = new object[2];
                record[0] = Datensatz.F1;
                record[1] = Datensatz.F2;
                return record;
            }
        }

        public Datensatz RowTransformationDB(Datensatz row)
        {
            row.F2 = row.F1 * -1;
            return row;
        }

        TableColumn Ziel_F1 => new TableColumn("F1", "int", allowNulls: true);
        TableColumn Ziel_F2 => new TableColumn("F2", "int", allowNulls: true);

        [TestMethod]
        public void TestDataflowDbToTb()
        {
            string ZielTabelle = "test.Staging3";
            CreateTableTask.Create(ZielTabelle, new List<TableColumn>() { Ziel_F1, Ziel_F2 });
            DBSource<Datensatz> DBSource = new DBSource<Datensatz> (
                ".", "ETLToolbox"
                , "SELECT 0 as F1" 
                + " UNION ALL SELECT 4 as F1" 
                + " UNION ALL SELECT -3 as F1"
                + " UNION ALL SELECT -2 as F1"
                );
            DBSource.DataMappingMethod = ReaderAdapter.Read;

            SQLDestination<Datensatz> Ziel_Schreibe = new SQLDestination<Datensatz>(".", "ETLToolbox");
            Ziel_Schreibe.DestinationTableName = ZielTabelle;
            Ziel_Schreibe.FieldCount = 2;
            Ziel_Schreibe.ObjectMappingMethod = WriterAdapter.Fill;
            Ziel_Schreibe.MaxBufferSize = 1000;

            DataFlowTask<Datensatz>.Execute("Test dataflow task", DBSource, Ziel_Schreibe, ZielTabelle, 10000, RowTransformationDB);
            Assert.AreEqual(4, SqlTask.ExecuteScalar<int>("Check staging table", string.Format("select count(*) from {0}", ZielTabelle)));
        }

       
    }

}
