using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ALE.ETLToolbox;
using System.Collections.Generic;
using System.Linq;
using System.Data;

namespace ALE.ETLToolboxTest.DataFlow
{
    [TestClass]
    public class TestDataFlowTask3
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


        [TestMethod]
        public void TestDataflow_Massendaten()
        {
            int Anzahl_je_Faktor = 10000;
            int Anzahl_Faktoren = 1000;

            string QuellTabelle = "test.source";
            CreateTableTask.Create(QuellTabelle, new List<TableColumn>() {
                new TableColumn("Key", "int", allowNulls: false, isPrimaryKey: true) { IsIdentity = true },
                new TableColumn("F1", "int", allowNulls: true),
                new TableColumn("F2", "int", allowNulls: true),
                new TableColumn("F3", "int", allowNulls: true),
                new TableColumn("F4", "int", allowNulls: true),
                new TableColumn("F5", "int", allowNulls: true),
                new TableColumn("F6", "int", allowNulls: true),
                new TableColumn("F7", "int", allowNulls: true),
                new TableColumn("F8", "int", allowNulls: true),
                new TableColumn("F9", "int", allowNulls: true),
                new TableColumn("F10", "int", allowNulls: true),});

            string sql_generate_Massendaten = @"
select top 0 F1,F2,F3,F4,F5,F6,F7,F8,F9,F10 into test.tmp from test.source -- tmp-Tabelle erstellen
declare @grenze as int = 10
declare @i as int = 0
while (@i < "+ Anzahl_je_Faktor + @")
begin
	insert into test.tmp
	select @i % @grenze, @i % @grenze + 1, @i % @grenze + 2, (@i % @grenze) * -1, (@i % @grenze) * -1 -1, @i % @grenze, @i % @grenze -1, @i % @grenze +2, @i% @grenze+3, @i % @grenze+4
	set @i = @i + 1
end

declare @j as int = 0
while (@j < " + Anzahl_Faktoren + @")
begin
	insert into test.source
	select F1,F2,F3,F4,F5,F6,F7,F8,F9,F10 from test.tmp
	set @j = @j + 1
end
"
;
            SqlTask.ExecuteNonQuery("Generiere Massendaten", sql_generate_Massendaten);




            //string ZielTabelle = "test.Staging4";
            //CreateTableTask.Create(ZielTabelle, new List<TableColumn>() { Ziel_F1, Ziel_F2 });
            //DBSource<Datensatz> DBSource = new DBSource<Datensatz>(
            //    ".", "ETLToolbox"
            //    , "SELECT 0 as F1"
            //    + " UNION ALL SELECT 4 as F1"
            //    + " UNION ALL SELECT -3 as F1"
            //    + " UNION ALL SELECT -2 as F1"
            //    );
            //DBSource.DataMappingMethod = ReaderAdapter.Read;

            //SQLDestination<Datensatz> Ziel_Schreibe = new SQLDestination<Datensatz>(".", "ETLToolbox");
            //Ziel_Schreibe.DestinationTableName = ZielTabelle;
            //Ziel_Schreibe.FieldCount = 2;
            //Ziel_Schreibe.ObjectMappingMethod = WriterAdapter.Fill;
            //Ziel_Schreibe.MaxBufferSize = 1000;

            //DataFlowTask<Datensatz>.Execute("Test dataflow task", DBSource, Ziel_Schreibe, ZielTabelle, 10000, RowTransformationDB);
            Assert.AreEqual(Anzahl_je_Faktor * Anzahl_Faktoren, SqlTask.ExecuteScalar<int>("Check staging table", string.Format("select count(*) from {0}", QuellTabelle)));

        }



    }
}
