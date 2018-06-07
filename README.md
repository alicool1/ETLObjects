# ETLObjects

Are you looking for a handy C# library to manage your ETL processes? Are you looking for an alternative for SSIS? Then read on.

## What is ETLObjects

ETLObjects will be the only C# class library that you need to do your whole ETL (or ELT). But no further talking, let's start with some code examples.

### Examples
Execute some sql on the DB
```
SqlTask.ExecuteNonQuery("Do some sql",$@"EXEC dbo.myProc");
```

Create or change a Stored Procedure
```
CRUDProcedureTask.CreateOrAlter("demo.proc1", "select 1 as test");
```
Create a schema and a table
```
CreateSchemaTask.Create("demo");
CreateTableTask.Create("demo.table1", new List<TableColumn>() {
    new TableColumn(name:"key",dataType:"int",allowNulls:false,isPrimaryKey:true, isIdentity:true),
    new TableColumn(name:"value", dataType:"nvarchar(100)",allowNulls:true)
});
```

Create your own dataflow - 
```
Graph g = new Graph();    
g.GetVertex(0, DBSource );
g.GetVertex(1, new RowTransformFunction<Datensatz>(RowTransformationDB));
g.GetVertex(2, destination );

g.AddEdge(0, 1); // connect 0 to 1
g.AddEdge(1, 2); // connect 1 to 2

DataFlowTask<Datensatz>.Execute("Test dataflow task", 10000, 1, g);
```

Logging is as easy as this
```
CreateLogTablesTask.CreateLog();
StartLoadProcessTask.Start("Process 1");
ControlFlow.STAGE = "Staging";
SqlTask.ExecuteNonQuery("some sql", "Select 1 as test");
ControlFlow.STAGE = "DataVault";
Sequence.Execute("some custom code", () => { });
LogTask.Warn("Some warning!");
EndLoadProcessTask.End("Everything successful");
```

but there is much more! 
CalculateDatabaseHashTask, CleanUpSchemaTask.CleanUp, CreateIndexTask, GetDatabaseListTask, RestoreDatabaseTask, XmlaTask, DropCubeTask, ProcessCubeTask, ConnectionManager, ControlFlow, FileConnection, AdoMD, AS, Package, CustomTask, more logging, ...         

## Getting Started

### Prerequisites

We recommend that you have Visual Studio 2017 installed (including the Github extension)

### Installing

Clone the repository
```
git clone https://github.com/roadrunnerlenny/ETLObjects.git
```

Open the download solution file ETLObjects.sln with Visual Studio.
Build or Run the solution. If you run it, the demo program will start.
To dig deeper into it, have a look at the ETLObjectsTest project. There is a test for everything that you can do with ETLObjects.


