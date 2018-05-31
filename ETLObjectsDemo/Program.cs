using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ETLObjects;
namespace ETLObjectsDemo
{
    class Program
    {
        static void Main(string[] args)
        {
            //Basics
            ControlFlow.CurrentDbConnection = new SqlConnectionManager(new ConnectionString("Data Source=.;Integrated Security=SSPI;"));
            DropDatabaseTask.Delete("DemoDB");
            ETLObjects.CreateDatabaseTask.Create("DemoDB");

            ControlFlow.CurrentDbConnection = new SqlConnectionManager(new ConnectionString("Data Source=.;Integrated Security=SSPI;Initial Catalog=DemoDB;"));
            CreateSchemaTask.Create("demo");
            CreateTableTask.Create("demo.table1", new List<TableColumn>() {
                new TableColumn(name:"key",dataType:"int",allowNulls:false,isPrimaryKey:true, isIdentity:true),
                new TableColumn(name:"value", dataType:"nvarchar(100)",allowNulls:true)
            });
           
            SqlTask.ExecuteNonQuery("Insert data",
               $@"insert into demo.table1 (value) select * from (values ('Ein Text'), ('Noch mehr Text')) as data(v)");

            int count = RowCountTask.Count("demo.table1").Value;
            Debug.WriteLine($"Found {count} entries in demo table!");

            //Truncate:
            //TruncateTableTask.Truncate("demo.table1");

            //Batch processing / Go keyword:
           
            //SqlTask.ExecuteNonQuery("sql with go keyword", @"insert into demo.table1 (value) select '####';
//go 2");

            ControlFlow.CurrentDbConnection = new SMOConnectionManager(new ConnectionString("Data Source=.;Integrated Security=SSPI;Initial Catalog=DemoDB;"));
            SqlTask.ExecuteNonQuery("sql with go keyword", @"insert into demo.table1 (value) select '####';
            go 2");

            AddFileGroupTask.AddFileGroup("FGName", "DemoDB", "200MB", "10MB", isDefaultFileGroup: true);

            CRUDProcedureTask.CreateOrAlter("demo.proc1", "select 1 as test");

            //Logging

            CreateLogTablesTask.CreateLog();
            StartLoadProcessTask.Start("Process 1");
            ControlFlow.STAGE = "Staging";
            SqlTask.ExecuteNonQuery("some sql", "Select 1 as test");
            TransferCompletedForLoadProcessTask.Complete();
            ControlFlow.STAGE = "DataVault";

            Sequence.Execute("some custom code", () => { });
            LogTask.Warn("Some warning!");
            EndLoadProcessTask.End("Everything successful");

            string jsonLP = GetLoadProcessAsJSONTask.GetJSON();
            string jsonLog = GetLogAsJSONTask.GetJSON(1);
            Console.ReadLine();
            
            //Further Tasks
            //CalculateDatabaseHashTask.Calculate(new List<string>() { "demo", "dbo" });
            //CleanUpSchemaTask.CleanUp("demo");
            //CreateIndexTask.Create("indexname","tablename", indexColumns)
            //GetDatabaseListTask.List();
            //RestoreDatabaseTask
            
            //Cube
            //XmlaTask.ExecuteNonQuery("some xmla","xmla goes here...")
            //DropCubeTask.Execute("Cube");
            //ProcessCubeTask.Process("Cube");

            //ConnectionManager & ControlFlow
            //FileConnection, AdoMD, AS
            //Package, CustomTask

            //Logging
            //AbortLoadProcessTask.Abourt()
            //CleanUpLogTask
            //RemoveLogTablesTask
            //ReadLodProcessTableTask
            //ReadLogTableTask

            //Dataflowtask -> ALI
        }
    }
}
