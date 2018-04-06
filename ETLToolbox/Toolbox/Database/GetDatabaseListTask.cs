using System;
using System.Collections.Generic;

namespace ALE.ETLToolbox {
    public class GetDatabaseListTask : GenericTask, ITask {
        /* ITask Interface */
        public override string TaskType { get; set; } = "GETDBLIST";
        public override string TaskName => $"Get names of all databases";
        public override void Execute() {
            DatabaseNames = new List<string>();
            new SqlTask(this, Sql) {
                Actions = new List<Action<object>>() {
                    n => DatabaseNames.Add((string)n)               
                }
            }.ExecuteReader();
        }


        public List<string> DatabaseNames { get; set; }      
        public string Sql {
            get {
                return $"SELECT [name] FROM master.dbo.sysdatabases WHERE dbid > 4";
            }
        }

        public GetDatabaseListTask() {

        }     
        
        public GetDatabaseListTask GetList() {
            Execute();
            return this;
        }

        public static List<string> List() => new GetDatabaseListTask().GetList().DatabaseNames;        

    }
}
