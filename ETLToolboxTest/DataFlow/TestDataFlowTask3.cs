using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ALE.ETLToolbox;
using System.Collections.Generic;
using System.Linq;
using System.Data;
using System.Diagnostics;

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
            public int Key;
            public int F1;
            public int F2;
            public int F3;
            public int F4;
            public int F5;
            public int F6;
            public int F7;
            public int F8;
            public int F9;
            public int F10;
            public int F1_calc;
            public int F2_calc;
            public int F3_calc;
            public int F4_calc;
            public int F5_calc;
            public int F6_calc;
            public int F7_calc;
            public int F8_calc;
            public int F9_calc;
            public int F10_calc;
        }

        public class ReaderAdapter
        {
            public static Datensatz Read(IDataRecord record)
            {
                var Datensatz = new Datensatz();
                Datensatz.Key = record.GetInt32(0);
                Datensatz.F1 = record.GetInt32(1);
                Datensatz.F2 = record.GetInt32(2);
                Datensatz.F3 = record.GetInt32(3);
                Datensatz.F4 = record.GetInt32(4);
                Datensatz.F5 = record.GetInt32(5);
                Datensatz.F6 = record.GetInt32(6);
                Datensatz.F7 = record.GetInt32(7);
                Datensatz.F8 = record.GetInt32(8);
                Datensatz.F9 = record.GetInt32(9);
                Datensatz.F10 = record.GetInt32(10);
                return Datensatz;
            }
        }

        private static int FieldCount = 21;
        public class WriterAdapter
        {
            public static object[] Fill(Datensatz Datensatz)
            {
                object[] record = new object[FieldCount];
                record[0] = Datensatz.Key;
                record[1] = Datensatz.F1;
                record[2] = Datensatz.F1_calc;
                record[3] = Datensatz.F2;
                record[4] = Datensatz.F2_calc;
                record[5] = Datensatz.F3;
                record[6] = Datensatz.F3_calc;
                record[7] = Datensatz.F4;
                record[8] = Datensatz.F4_calc;
                record[9] = Datensatz.F5;
                record[10] = Datensatz.F5_calc;
                record[11] = Datensatz.F6;
                record[12] = Datensatz.F6_calc;
                record[13] = Datensatz.F7;
                record[14] = Datensatz.F7_calc;
                record[15] = Datensatz.F8;
                record[16] = Datensatz.F8_calc;
                record[17] = Datensatz.F9;
                record[18] = Datensatz.F9_calc;
                record[19] = Datensatz.F10;
                record[20] = Datensatz.F10_calc;
                return record;
            }
        }

        public Datensatz RowTransformationDB(Datensatz row)
        {
 
            row.F1_calc = IntervallPunktSuche.IntervallpunktSucheLinear(MetrischeSkala, (decimal)row.F1).IntervallScore;
            row.F2_calc = IntervallPunktSuche.IntervallpunktSucheLinear(MetrischeSkala, (decimal)row.F2).IntervallScore;
            row.F3_calc = IntervallPunktSuche.IntervallpunktSucheLinear(MetrischeSkala, (decimal)row.F3).IntervallScore;
            row.F4_calc = IntervallPunktSuche.IntervallpunktSucheLinear(MetrischeSkala, (decimal)row.F4).IntervallScore;
            row.F5_calc = IntervallPunktSuche.IntervallpunktSucheLinear(MetrischeSkala, (decimal)row.F5).IntervallScore;
            row.F6_calc = IntervallPunktSuche.IntervallpunktSucheLinear(MetrischeSkala, (decimal)row.F6).IntervallScore;
            row.F7_calc = IntervallPunktSuche.IntervallpunktSucheLinear(MetrischeSkala, (decimal)row.F7).IntervallScore;
            row.F8_calc = IntervallPunktSuche.IntervallpunktSucheLinear(MetrischeSkala, (decimal)row.F8).IntervallScore;
            row.F9_calc = IntervallPunktSuche.IntervallpunktSucheLinear(MetrischeSkala, (decimal)row.F9).IntervallScore;
            row.F10_calc = IntervallPunktSuche.IntervallpunktSucheLinear(MetrischeSkala, (decimal)row.F10).IntervallScore;

            return row;
        }

        List<IntervallPunktMetrisch> MetrischeSkala = new List<IntervallPunktMetrisch>();

        [TestMethod]
        public void TestDataflow_Massendaten()
        {

            int SkalaGrenze = 10000;

            for (int i = SkalaGrenze * -1; i < SkalaGrenze; i = i + 50)
            {
                MetrischeSkala.Add(new IntervallPunktMetrisch(i, i));
            }

           
            int Anzahl_je_Faktor = 10000;
            int Anzahl_Faktoren = 50;

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
select top 0 F1,F2,F3,F4,F5,F6,F7,F8,F9,F10 into test.tmp from "+ QuellTabelle + @" -- tmp-Tabelle erstellen
declare @grenze as int = "+ SkalaGrenze + @"
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
	insert into "+ QuellTabelle + @"
	select F1,F2,F3,F4,F5,F6,F7,F8,F9,F10 from test.tmp
	set @j = @j + 1
end
"
;
            Debug.WriteLine("Generiere Massendaten ... ");

            SqlTask.ExecuteNonQuery("Generiere Massendaten", sql_generate_Massendaten);

            
            string ZielTabelle = "test.destination";
            CreateTableTask.Create(ZielTabelle, new List<TableColumn>() {
                new TableColumn("Key", "int", allowNulls: false, isPrimaryKey: true) { IsIdentity = false },
                new TableColumn("F1", "int", allowNulls: true), new TableColumn("F1_calc", "int", allowNulls: true),
                new TableColumn("F2", "int", allowNulls: true), new TableColumn("F2_calc", "int", allowNulls: true),
                new TableColumn("F3", "int", allowNulls: true), new TableColumn("F3_calc", "int", allowNulls: true),
                new TableColumn("F4", "int", allowNulls: true), new TableColumn("F4_calc", "int", allowNulls: true),
                new TableColumn("F5", "int", allowNulls: true), new TableColumn("F5_calc", "int", allowNulls: true),
                new TableColumn("F6", "int", allowNulls: true), new TableColumn("F6_calc", "int", allowNulls: true),
                new TableColumn("F7", "int", allowNulls: true), new TableColumn("F7_calc", "int", allowNulls: true),
                new TableColumn("F8", "int", allowNulls: true), new TableColumn("F8_calc", "int", allowNulls: true),
                new TableColumn("F9", "int", allowNulls: true), new TableColumn("F9_calc", "int", allowNulls: true),
                new TableColumn("F10", "int", allowNulls: true), new TableColumn("F10_calc", "int", allowNulls: true),});


            DBSource<Datensatz> DBSource = new DBSource<Datensatz>(
                ".", "ETLToolbox"
                , string.Format("select [Key],F1,F2,F3,F4,F5,F6,F7,F8,F9,F10 from {0}", QuellTabelle)
                );
            DBSource.DataMappingMethod = ReaderAdapter.Read;

            SQLDestination<Datensatz> Ziel_Schreibe = new SQLDestination<Datensatz>(".", "ETLToolbox");
            Ziel_Schreibe.DestinationTableName = ZielTabelle;
            Ziel_Schreibe.FieldCount = FieldCount;
            Ziel_Schreibe.ObjectMappingMethod = WriterAdapter.Fill;
            Ziel_Schreibe.MaxBufferSize = 1000;

            int MaxDegreeOfParallelism = 1;
            SqlTask.ExecuteNonQuery("truncate Zieltabelle", string.Format("truncate table {0}", ZielTabelle));
            Debug.WriteLine("Start Laufzeittest MaxDegreeOfParallelism {0} ... ", MaxDegreeOfParallelism);
            Stopwatch s = Stopwatch.StartNew();
            DataFlowTask<Datensatz>.Execute("Test dataflow task", DBSource, Ziel_Schreibe, ZielTabelle, 10000, MaxDegreeOfParallelism, RowTransformationDB);
            Debug.WriteLine("Laufzeit in ms: {0}", s.ElapsedMilliseconds);

            MaxDegreeOfParallelism = 10;
            SqlTask.ExecuteNonQuery("truncate Zieltabelle", string.Format("truncate table {0}", ZielTabelle));
            Debug.WriteLine("Start Laufzeittest MaxDegreeOfParallelism {0} ... ", MaxDegreeOfParallelism);
            s = Stopwatch.StartNew();
            DataFlowTask<Datensatz>.Execute("Test dataflow task", DBSource, Ziel_Schreibe, ZielTabelle, 10000, MaxDegreeOfParallelism, RowTransformationDB);
            Debug.WriteLine("Laufzeit in ms: {0}", s.ElapsedMilliseconds);

            MaxDegreeOfParallelism = 20;
            SqlTask.ExecuteNonQuery("truncate Zieltabelle", string.Format("truncate table {0}", ZielTabelle));
            Debug.WriteLine("Start Laufzeittest MaxDegreeOfParallelism {0} ... ", MaxDegreeOfParallelism);
            s = Stopwatch.StartNew();
            DataFlowTask<Datensatz>.Execute("Test dataflow task", DBSource, Ziel_Schreibe, ZielTabelle, 10000, MaxDegreeOfParallelism, RowTransformationDB);
            Debug.WriteLine("Laufzeit in ms: {0}", s.ElapsedMilliseconds);


            Assert.AreEqual(Anzahl_je_Faktor * Anzahl_Faktoren, SqlTask.ExecuteScalar<int>("Check staging table", string.Format("select count(*) from {0}", QuellTabelle)));

        }



    }
}
