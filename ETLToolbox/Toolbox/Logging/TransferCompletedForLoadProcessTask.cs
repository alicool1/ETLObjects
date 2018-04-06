using System;


namespace ALE.ETLToolbox {
    public class TransferCompletedForLoadProcessTask : GenericTask, ITask {
        /* ITask Interface */
        public override string TaskType { get; set; } = "TRANSFERCOMPLETE";
        public override string TaskName => $"Set transfer completed for {LoadProcessKey}";
        public override void Execute() {
            new SqlTask(this, Sql).ExecuteNonQuery();            
            var rlp = new ReadLoadProcessTableTask(LoadProcessKey) { TaskType = this.TaskType, TaskHash = this.TaskHash, DisableLogging = true };
            rlp.Execute();
            ControlFlow.CurrentLoadProcess = rlp.LoadProcess;
        }

        /* Public properties */
        public int? _loadProcessKey;
        public int? LoadProcessKey {
            get {
                return _loadProcessKey ?? ControlFlow.CurrentLoadProcess?.LoadProcessKey;
            }
            set {
                _loadProcessKey = value;
            }
        }

        public string Sql => $@"EXECUTE etl.TransferCompletedForLoadProcess
	 @LoadProcessKey = '{LoadProcessKey}'";

        public TransferCompletedForLoadProcessTask() {

        }

        public TransferCompletedForLoadProcessTask(int? loadProcessKey) : this() {
            this.LoadProcessKey = loadProcessKey;
        }

        public static void Complete() => new TransferCompletedForLoadProcessTask().Execute();
        public static void Complete(int? loadProcessKey) => new TransferCompletedForLoadProcessTask(loadProcessKey).Execute();


    }
}
