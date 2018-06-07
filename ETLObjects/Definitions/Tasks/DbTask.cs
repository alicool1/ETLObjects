using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Security.Cryptography;
using System.Text;

namespace ETLObjects {
    public abstract class DbTask : GenericTask {

        /* Public Properties */
        public string Sql { get; set; }
        public FileConnectionManager FileConnection { get; set; }
        public List<Action<object>> Actions { get; set; }
        public Action BeforeRowReadAction { get; set; }
        public Action AfterRowReadAction { get; set; }
        public long ReadTopX { get; set; } = long.MaxValue;
        public int? RowsAffected { get; private set; }

        public bool DisableExtension { get; set; }
        public string Command {
            get {
                if (HasSql)
                    return HasName ? NameAsComment + Sql : Sql;
                else if (HasFileConnection) {
                    if (FileConnection.FileExists)
                        return HasName ? NameAsComment + FileConnection.ReadContent() : FileConnection.ReadContent();
                    else {
                        NLogger.Warn($"Sql file was not found: {FileConnection.FileName}", TaskType, "RUN", TaskHash, ControlFlow.STAGE);
                        return $"SELECT 'File {FileConnection.FileName} not found'";
                    }
                }
                else
                    throw new Exception("Empty command");
            }
        }

        /* Internal/Private properties */
        internal bool DoSkipSql { get; private set; }
        NLog.Logger NLogger { get; set; }
        bool HasSql => !(String.IsNullOrWhiteSpace(Sql));
        bool HasFileConnection => FileConnection != null;

        /* Some constructors */
        public DbTask() {
            NLogger = NLog.LogManager.GetLogger("Default");
        }

        public DbTask(string name) : this() {
            this.TaskName = name;
        }

        public DbTask(string name, string sql) : this(name) {
            this.Sql = sql;
        }

        public DbTask(ITask callingTask, string sql) : this() {
            TaskName = callingTask.TaskName;
            TaskHash = callingTask.TaskHash;        
            ConnectionManager = callingTask.ConnectionManager;
            TaskType = callingTask.TaskType;
            DisableLogging = callingTask.DisableLogging;
            this.Sql = sql;
        }

        public DbTask(string name, string sql, params Action<object>[] actions) : this(name, sql) {
            Actions = actions.ToList();
        }

        public DbTask(string name, string sql, Action beforeRowReadAction, Action afterRowReadAction, params Action<object>[] actions) : this(name, sql) {
            BeforeRowReadAction = beforeRowReadAction;
            AfterRowReadAction = afterRowReadAction;
            Actions = actions.ToList();
        }

        public DbTask(string name, FileConnectionManager fileConnection) : this(name) {
            this.FileConnection = fileConnection;
        }

        /* Public methods */
        public int ExecuteNonQuery() {
            using (var conn= DbConnectionManager.Clone()) {
                conn.Open();
                QueryStart();
                RowsAffected = DoSkipSql ? 0 : conn.ExecuteNonQuery(Command);//DbConnectionManager.ExecuteNonQuery(Command);
                QueryFinish(LogType.Rows);
            }
            return RowsAffected ?? 0;
        }

        public object ExecuteScalar() {
            object result = null;
            using (var conn = DbConnectionManager.Clone()) {
                conn.Open();
                QueryStart();
                result = conn.ExecuteScalar(Command);
                QueryFinish();
            }
            return result;
        }

        public Nullable<T> ExecuteScalar<T>() where T : struct {
            object result = ExecuteScalar();
            if (result == null || result == DBNull.Value)
                return null;
            else
                return ((T)result);
        }


        public bool ExecuteScalarAsBool() {
            int? result = ExecuteScalar<int>();
            return IntToBool(result);
        }

        public void ExecuteReader() {
            using (var conn = DbConnectionManager.Clone()) {
                conn.Open();
                QueryStart();
                //SqlDataReader reader = ConnectionManager.ExecuteReader(Command) as SqlDataReader;
                IDataReader reader = conn.ExecuteReader(Command) as IDataReader;
                for (int row = 0; row < ReadTopX; row++) {
                    if (reader.Read()) {
                        BeforeRowReadAction?.Invoke();
                        for (int i = 0; i < Actions.Count; i++) {
                            if (!reader.IsDBNull(i))
                                Actions[i].Invoke(reader.GetValue(i));
                            else
                                Actions[i].Invoke(null);
                        }
                        AfterRowReadAction?.Invoke();
                    }
                    else {
                        break;
                    }
                }
                reader.Close();
                QueryFinish();
            }
        }


        public void BulkInsert(IDataReader data, IColumnMappingCollection columnMapping, string tableName) {
            using (var conn = DbConnectionManager.Clone()) {
                conn.Open();
                QueryStart(LogType.Bulk);
                conn.BulkInsert(data, columnMapping, tableName);
                RowsAffected = data.RecordsAffected;
                QueryFinish(LogType.Bulk);
            }
        }


        /* Private implementation & stuff */
        enum LogType {
            None,
            Rows,
            Bulk
        }

        static bool IntToBool(int? result) {
            if (result != null && result > 0)
                return true;
            else
                return false;
        }

        void QueryStart(LogType logType = LogType.None) {
            if (!DisableLogging)
                LoggingStart(logType);

            if (!DisableExtension)
                ExecuteExtension();
        }

        void QueryFinish(LogType logType = LogType.None) {
            if (!DisableLogging)
                LoggingEnd(logType);
        }

        void LoggingStart(LogType logType) {
            NLogger.Info(TaskName, TaskType, "START", TaskHash, ControlFlow.STAGE, ControlFlow.CurrentLoadProcess?.LoadProcessKey);
            if (logType == LogType.Bulk)
                NLogger.Debug($"SQL Bulk Insert", TaskType, "RUN", TaskHash, ControlFlow.STAGE, ControlFlow.CurrentLoadProcess?.LoadProcessKey);
            else
                NLogger.Debug($"{Command}", TaskType, "RUN", TaskHash, ControlFlow.STAGE, ControlFlow.CurrentLoadProcess?.LoadProcessKey);
        }

        void LoggingEnd(LogType logType) {
            NLogger.Info(TaskName, TaskType, "END", TaskHash, ControlFlow.STAGE, ControlFlow.CurrentLoadProcess?.LoadProcessKey);
            if (logType == LogType.Rows)
                NLogger.Debug($"Rows affected: {RowsAffected ?? 0}", TaskType, "RUN", TaskHash, ControlFlow.STAGE, ControlFlow.CurrentLoadProcess?.LoadProcessKey);
        }

        void ExecuteExtension() {
            if (ExtensionFileLoader.ExistsFolder && HasName) {
                List<ExtensionFile> extFiles = ExtensionFileLoader.GetExtensionFiles(TaskHash);

                if (extFiles.Count > 0) {
                    foreach (var extFile in extFiles) {
                        new SqlTask($"Extensions: {extFile.Name}", new FileConnectionManager(extFile.FileName)) {
                            ConnectionManager = this.ConnectionManager,
                            DisableExtension = true
                        }.ExecuteNonQuery();
                    }
                    DoSkipSql = extFiles.Any(ef => ef.HasSkipNextStatement);
                }
            }
        }


    }


}
