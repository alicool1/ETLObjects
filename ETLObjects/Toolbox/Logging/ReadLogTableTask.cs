using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ETLObjects {
    public class ReadLogTableTask : GenericTask, ITask {
        /* ITask Interface */
        public override string TaskType { get; set; } = "LOG_READLOG";
        public override string TaskName => $"Read all log entries for {LoadProcessKey ?? 0 }";
        public override void Execute() {
            LogEntries = new List<LogEntry>();
            LogEntry current = new LogEntry();
            new SqlTask(this, Sql) {
                DisableLogging = true,
                DisableExtension = true,
                BeforeRowReadAction = () => current = new LogEntry(),
                AfterRowReadAction = () => LogEntries.Add(current),
                Actions = new List<Action<object>>() {
                    col => current.LogKey = (int)col,
                    col => current.LogDate = (DateTime)col,
                    col => current.Level = (string)col,
                    col => current.Message = (string)col,
                    col => current.TaskType = (string)col,
                    col => current.TaskAction = (string)col,
                    col => current.TaskHash = (string)col,
                    col => current.Stage = (string)col,
                    col => current.Source = (string)col,
                    col => current.LoadProcessKey = (int?)col,
                }
            }.ExecuteReader();            
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

        public ReadLogTableTask ReadLog() {
            Execute();
            return this;
        }

        public List<LogEntry> LogEntries { get; private set; }

        public string Sql => $@"select LogKey, LogDate, Level, Message, TaskType, TaskAction, TaskHash, Stage, Source, LoadProcessKey
    from etl.Log" +
            (LoadProcessKey != null ? $@" where LoadProcessKey = {LoadProcessKey}"
            : "");
        
        public ReadLogTableTask() {

        }

        public ReadLogTableTask(int? loadProcessKey) : this() {
            this.LoadProcessKey = loadProcessKey;
        }

        public static List<LogEntry> Read() => new ReadLogTableTask().ReadLog().LogEntries;
        public static List<LogEntry> Read(int? loadProcessKey) => new ReadLogTableTask(loadProcessKey).ReadLog().LogEntries;

    }
}
