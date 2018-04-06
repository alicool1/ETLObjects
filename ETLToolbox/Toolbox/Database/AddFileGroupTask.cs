using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ALE.ETLToolbox {
    public class AddFileGroupTask : GenericTask, ITask {
        /* ITask Interface */
        public override string TaskType { get; set; } = "CREATEFG";
        public override string TaskName => $"Create Filegroup {FileGroupName}";
        public override void Execute() => new SqlTask(this, Sql).ExecuteNonQuery();

        /* Public properties */
        public string FileGroupName { get; set; }
        public string DatabaseName { get; set; }
        public string Size { get; set; }
        public string Filegrowth { get; set; }
        public bool IsDefaultFileGroup { get; set; }
        public string Sql {
            get {
                return $@"--Add the filegroup
  declare @sql nvarchar(4000) = N'select 1'
  alter database [{DatabaseName}] add filegroup [{FileGroupName}]

  declare @path nvarchar(500)
  select @path = substring([physical_name],0, len([physical_name]) - charindex('\', reverse([physical_name]))+1 ) + '\{FileGroupName}.ndf'
  from sys.master_files 
  where name = '{DatabaseName}'
  
  set @sql = N'
    alter database [{DatabaseName}] add file ( 
      name = N''{FileGroupName}''
      , filename = N''' + @path + '''	
      , size = {Size}
      , filegrowth = {Filegrowth}
    ) 
    to filegroup [{FileGroupName}]'  
  exec sp_executesql @stmt=@sql
  
  set @sql = N'use [{DatabaseName}]'
  exec sp_executesql @stmt=@sql

  {SetDefaultFileGroupSql}
";
            }
        }

        public AddFileGroupTask() { }

        public AddFileGroupTask(string fileGroupName, string databaseName) : this() {
            FileGroupName = fileGroupName;
            DatabaseName = databaseName;
        }

        public AddFileGroupTask(string name, string databaseName, string size, string filegrowth, bool isDefaultFileGroup) : this(name, databaseName) {
            Size = size;
            Filegrowth = filegrowth;
            IsDefaultFileGroup = isDefaultFileGroup;
        }

        public static void AddFileGroup(string fileGroupName, string databaseName) => new AddFileGroupTask(fileGroupName, databaseName).Execute();

        public static void AddFileGroup(string fileGroupName, string databaseName, string size, string fileGrowth, bool isDefaultFileGroup)
            => new AddFileGroupTask(fileGroupName, databaseName, size, fileGrowth, isDefaultFileGroup).Execute();

        private string SetDefaultFileGroupSql => IsDefaultFileGroup ?
             $@"--if not exists (select name from sys.filegroups where is_default = 1 and name = N'{FileGroupName}')    
  alter database [{DatabaseName}] MODIFY FILEGROUP [{FileGroupName}] default" : String.Empty;


    }
}
