using System;


namespace ALE.ETLToolbox {
    public class RowCountTask : GenericTask, ITask {
        /* ITask Interface */
        public override string TaskType { get; set; } = "ROWCOUNT";
        public override string TaskName => $"Count Rows for {TableName}" + (HasCondition ? $" with condition {Condition}" : "");
        public override void Execute() {
            Rows = new SqlTask(this, Sql).ExecuteScalar<int>();
        }


        public string TableName { get; set; }
        public string Condition { get; set; }
        public bool HasCondition => !String.IsNullOrWhiteSpace(Condition);
        public int? Rows { get; private set; }
        public bool? HasRows => Rows > 0;
        public string Sql {
            get {
                return $"select count(*) from {TableName} {WhereClause} {Condition}";
            }
        }

        public RowCountTask() {

        }
        public RowCountTask(string tableName) {
            this.TableName = tableName;
        }

        public RowCountTask(string tableName, string condition) : this(tableName) {
            this.Condition = condition;
        }

        public RowCountTask Count() {
            Execute();
            return this;
        }

        public static int? Count(string tableName) => new RowCountTask(tableName).Count().Rows;

        public static int? Count(string tableName, string condition) => new RowCountTask(tableName, condition).Count().Rows;

        string WhereClause => HasCondition ? "where" : String.Empty;

    }
}
