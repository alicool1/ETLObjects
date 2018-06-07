using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Security.Cryptography;
using System.Text;

namespace ETLObjects {
    public class SqlTask : DbTask {
        public override string TaskType { get; set; } = "SQL";
        public override string TaskName { get; set; } = "Run some sql";
        public override void Execute() => ExecuteNonQuery();
        public SqlTask() {
        }

        public SqlTask(string name) : base(name) {
        }

        public SqlTask(string name, FileConnectionManager fileConnection) : base(name, fileConnection) {
        }

        public SqlTask(ITask callingTask, string sql) : base(callingTask, sql) {
        }

        public SqlTask(string name, string sql) : base(name, sql) {
        }

        public SqlTask(string name, string sql, params Action<object>[] actions) : base(name, sql, actions) {
        }

        public SqlTask(string name, string sql, Action beforeRowReadAction, Action afterRowReadAction, params Action<object>[] actions) : base(name, sql, beforeRowReadAction, afterRowReadAction, actions) {
        }

        /* Static methods for convenience */
        public static int ExecuteNonQuery(string name, string sql) => new SqlTask(name, sql).ExecuteNonQuery();
        public static int ExecuteNonQuery(string name, FileConnectionManager fileConnection) => new SqlTask(name, fileConnection).ExecuteNonQuery();
        public static object ExecuteScalar(string name, string sql) => new SqlTask(name, sql).ExecuteScalar();
        public static Nullable<T> ExecuteScalar<T>(string name, string sql) where T : struct => new SqlTask(name, sql).ExecuteScalar<T>();
        public static bool ExecuteScalarAsBool(string name, string sql) => new SqlTask(name, sql).ExecuteScalarAsBool();
        public static void ExecuteReaderSingleLine(string name, string sql, params Action<object>[] actions) =>
           new SqlTask(name, sql, actions) { ReadTopX = 1 }.ExecuteReader();
        public static void ExecuteReader(string name, string sql, params Action<object>[] actions) => new SqlTask(name, sql, actions).ExecuteReader();
        public static void ExecuteReader(string name, string sql, Action beforeRowReadAction, Action afterRowReadAction, params Action<object>[] actions) =>
            new SqlTask(name, sql, beforeRowReadAction, afterRowReadAction, actions).ExecuteReader();
        public static void BulkInsert(string name, IDataReader data, IColumnMappingCollection columnMapping, string tableName) =>
            new SqlTask(name).BulkInsert(data, columnMapping, tableName);
    }

}
