using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;

namespace ETLObjects {
    public class CreateTableTask : GenericTask, ITask {
        /* ITask Interface */
        public override string TaskType { get; set; } = "CREATETABLE";
        public override string TaskName => $"Create table {TableName}";
        public override void Execute() => new SqlTask(this, Sql).ExecuteNonQuery();

        /* Public properties */
        public string TableName { get; set; }
        public string TableWithoutSchema => TableName.IndexOf('.') > 0 ? TableName.Substring(TableName.LastIndexOf('.') + 1) : TableName;
        public IList<ITableColumn> Columns { get; set; }

        public bool OnlyNVarCharColumns { get; set; }
        public string Sql => $@"
if object_id('{TableName}', 'U') is null
  create table {TableName} (
  {ColumnsDefinitionSql}
  )
";

        public CreateTableTask() {

        }
        public CreateTableTask(string tableName, IList<ITableColumn> columns) : this() {
            this.TableName = tableName;
            this.Columns = columns;
        }

        public static void Create(string tableName, IList<ITableColumn> columns) => new CreateTableTask(tableName, columns).Execute();
        public static void Create(string tableName, List<TableColumn> columns) => new CreateTableTask(tableName, columns.Cast<ITableColumn>().ToList()).Execute();

        string ColumnsDefinitionSql => String.Join("  , " + Environment.NewLine, Columns?.Select(col => CreateTableDefinition(col)));

        string CreateTableDefinition(ITableColumn col) {

            string dataType = string.Empty;
            if (String.IsNullOrWhiteSpace(col.ComputedColumn))
                dataType = OnlyNVarCharColumns ? $"nvarchar({DataTypeConverter.GetTypeLength(col.DataType)})" : col.DataType;
            string identitySql = col.IsIdentity ? $"identity({col.IdentitySeed ?? 1},{col.IdentityIncrement ?? 1})" : string.Empty;
            string collationSql = !String.IsNullOrWhiteSpace(col.Collation) ? $"collate {col.Collation}" : string.Empty;
            string nullSql = string.Empty;
            if (String.IsNullOrWhiteSpace(col.ComputedColumn))
                nullSql = col.AllowNulls ? "NULL" : "NOT NULL";            
            string primarySql = col.IsPrimaryKey ? $"constraint [pk_{TableWithoutSchema}_{col.Name}] primary key clustered ( [{col.Name}] asc )" : string.Empty;
            string defaultSql = string.Empty;
            if (!col.IsPrimaryKey)            
                defaultSql = col.DefaultValue != null ? DefaultConstraintName(col.DefaultConstraintName) +  $" default {SetQuotesIfString(col.DefaultValue)}" : string.Empty;                        
            string computedColumnSql = !String.IsNullOrWhiteSpace(col.ComputedColumn) ? $"as {col.ComputedColumn}" : string.Empty;
            return $@"[{col.Name}] {dataType} {identitySql} {collationSql} {nullSql} {primarySql} {defaultSql} {computedColumnSql}";
        }

        string DefaultConstraintName(string defConstrName) => !String.IsNullOrWhiteSpace(defConstrName) ? $"constraint {defConstrName}" : string.Empty;

        string SetQuotesIfString(string value) {
            if (!Regex.IsMatch(value, @"^\d+(\.\d+|)$"))//@" ^ (\d|\.)+$"))
                return $"'{value}'";
            else
                return value;
                
        }
    }
}
