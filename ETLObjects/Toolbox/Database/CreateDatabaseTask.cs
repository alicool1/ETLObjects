using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ETLObjects
{
    public class CreateDatabaseTask : GenericTask, ITask
    {
        /* ITask Interface */
        public override string TaskType { get; set; } = "CREATEDB";
        public override string TaskName => $"Create DB {DatabaseName}";       
        public override void Execute()
        {
            new SqlTask(this, Sql).ExecuteNonQuery();
            
        }
     

        /* Public properties */
        public string DatabaseName { get; set; }
        public RecoveryModel RecoveryModel { get; set; } = RecoveryModel.Simple;      
        public string Collation { get; set; }
        public string Sql
        {
            get
            {
                return
    $@"
if (db_id('{DatabaseName}') is null)
begin 
  use [master]
  --Create Database
  create database [{DatabaseName}] {CollationString}
  {RecoveryString}  
  alter database [{DatabaseName}] set auto_create_statistics on
  alter database [{DatabaseName}] set auto_update_statistics on
  alter database [{DatabaseName}] set auto_update_statistics_async off
  alter database [{DatabaseName}] set auto_close off
  alter database [{DatabaseName}] set auto_shrink off
  
  --wait for database to enter 'ready' state
  declare @dbReady bit = 0
  while (@dbReady = 0)
  begin
    select @dbReady = case when DATABASEPROPERTYEX('{DatabaseName}', 'Collation') is null then 0 else 1 end                    
  end  
end
";
            }
        }

        /* Some constructors */
        public CreateDatabaseTask() {
        }

        public CreateDatabaseTask(string databaseName) : this()
        {
            DatabaseName = databaseName;
        }
       
        public CreateDatabaseTask(string databaseName, RecoveryModel recoveryModel) : this(databaseName)
        {
            RecoveryModel = recoveryModel;
        }

        public CreateDatabaseTask(string databaseName, RecoveryModel recoveryModel, string collation) : this(databaseName, recoveryModel)
        {
            Collation = collation;
        }

        /* Static methods for convenience */
        public static void Create(string databaseName) => new CreateDatabaseTask(databaseName).Execute();
        public static void Create(string databaseName, RecoveryModel recoveryModel) => new CreateDatabaseTask(databaseName,recoveryModel).Execute();
        public static void Create(string databaseName, RecoveryModel recoveryModel, string collation) => new CreateDatabaseTask(databaseName, recoveryModel,collation).Execute();

        /* Implementation & stuff */
        string RecoveryModelAsString
        {
            get
            {
                if (RecoveryModel == RecoveryModel.Simple)
                    return "simple";
                else if (RecoveryModel == RecoveryModel.BulkLogged)
                    return "bulk";
                else if (RecoveryModel == RecoveryModel.Full)
                    return "full";
                else return string.Empty;
            }
        }
        bool HasCollation => !String.IsNullOrWhiteSpace(Collation);
        string CollationString => HasCollation ? "collate " + Collation : string.Empty;
        string RecoveryString => RecoveryModel != RecoveryModel.Default ?
            $"alter database [{DatabaseName}] set recovery {RecoveryModelAsString} with no_wait"
            : string.Empty;
       
    }

    public enum RecoveryModel
    {
        Default, Simple, BulkLogged, Full
    }  

}
