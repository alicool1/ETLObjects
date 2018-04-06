using System;
using System.Collections.Generic;

namespace ALE.ETLToolbox {
    public class CreateIndexTask: GenericTask, ITask {
        /* ITask Interface */
        public override string TaskType { get; set; } = "CREATEINDEX";
        public override string TaskName => $"Create index {IndexName} on table {TableName}";
        public override void Execute() => new SqlTask(this, Sql).ExecuteNonQuery();

        /* Public properties */
        public string IndexName { get; set; }
        public string TableName { get; set; }
        public IList<string> IndexColumns { get; set; }
        public IList<string> IncludeColumns { get; set; }
        public bool IsUnique { get; set; }
        public bool IsClustered { get; set; }
        public string Sql => $@"
if not exists (select *  from sys.indexes  where name='{IndexName}' and object_id = object_id('{TableName}'))
  create {UniqueSql} {ClusteredSql} index {IndexName} on {TableName}
  ( {String.Join(",", IndexColumns)} )
  {IncludeSql}
  with(PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = ON, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)
";

        public CreateIndexTask() {

        }
        public CreateIndexTask(string indexName, string tableName, IList<string> indexColumns) : this() {
            this.IndexName = indexName;
            this.TableName = tableName;
            this.IndexColumns = indexColumns;
        }

        public CreateIndexTask(string indexName, string tableName, IList<string> indexColumns, IList<string> includeColumns) : this(indexName, tableName, indexColumns) {
            this.IncludeColumns = includeColumns;
        }

        public static void Create(string indexName, string tableName, IList<string> indexColumns) => new CreateIndexTask(indexName,tableName,indexColumns).Execute();
        public static void Create(string indexName, string tableName, IList<string> indexColumns, IList<string> includeColumns) => new CreateIndexTask(indexName, tableName, indexColumns, includeColumns).Execute();

        string UniqueSql => IsUnique ? "unique" : string.Empty;
        string ClusteredSql => IsClustered ? "clustered" : "nonclustered";
        string IncludeSql => IncludeColumns?.Count > 0 ? $"include ({String.Join("  ,", IncludeColumns)})" : string.Empty;

    }
}
