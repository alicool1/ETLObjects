using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.IO;

namespace ETLObjects
{
    public class ProcessCubeTask : GenericTask, ITask
    {
        public override string TaskType { get; set; } = "PROCESSCUBE";
        public override string TaskName => $"Process cube {ASConnectionManager.ConnectionString.CatalogName}";
        public override void Execute() {
            NLogger.Info(TaskName, TaskType, "START", TaskHash, ControlFlow.STAGE, ControlFlow.CurrentLoadProcess?.LoadProcessKey);
            using (var conn = ASConnectionManager.Clone()) {
                conn.Open();
                conn.Process();
            }
            NLogger.Info(TaskName, TaskType, "END", TaskHash, ControlFlow.STAGE, ControlFlow.CurrentLoadProcess?.LoadProcessKey);
        }       
        
        public ProcessCubeTask()
        {
            NLogger = NLog.LogManager.GetLogger("Default");
        }

        public ProcessCubeTask(string name) : this()
        {          
            this.TaskName = name;
        }              

        public static void Process(string name) => new ProcessCubeTask(name).Execute();

        NLog.Logger NLogger { get; set; }
    }
}
