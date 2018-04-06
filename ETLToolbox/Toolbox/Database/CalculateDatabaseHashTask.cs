using System;
using System.Collections.Generic;
using System.Linq;

namespace ALE.ETLToolbox {
    public class CalculateDatabaseHashTask : GenericTask, ITask {
        /* ITask Interface */
        public override string TaskType { get; set; } = "CALCDBHASH";
        public override string TaskName => $"Calculate hash value for schema(s) {SchemaNamesAsString}";
        public override void Execute() {
            List<string> allColumns = new List<string>();
            new SqlTask(this, Sql) {
                Actions = new List<Action<object>>() {
                    col => allColumns.Add((string)col)
                }
            }
                .ExecuteReader();
            DatabaseHash = HashHelper.Encrypt_Char40(String.Join("|", allColumns));
        }

        /* Public properties */
        public List<string> SchemaNames { get; set; }

        public string DatabaseHash { get; private set; }

        string SchemaNamesAsString => String.Join(",",SchemaNames.Select(name=>$"'{name}'"));
    public string Sql => $@"
select sch.name + '.' + tbls.name + N'|' + 
	   cols.name + N'|' + 
	   typ.name + N'|' + 
	   cast(cols.max_length as nvarchar(20))+ N'|' + 
	   cast(cols.precision as nvarchar(20)) + N'|' + 
	   cast(cols.scale as nvarchar(20)) + N'|' + 
	   cast(cols.is_nullable as nvarchar(3)) + N'|' + 
	   cast(cols.is_identity as nvarchar(3))+ N'|' + 
	   cast(cols.is_computed as nvarchar(3)) as FullColumnName
from sys.columns cols
inner join sys.tables tbls on cols.object_id = tbls.object_id
inner join sys.schemas sch on sch.schema_id = tbls.schema_id
inner join sys.types typ on typ.user_type_id = cols.user_type_id
where tbls.type = 'U'
and sch.name in ({SchemaNamesAsString})
order by sch.name, tbls.name, cols.column_id
";

        public CalculateDatabaseHashTask() {

        }
        public CalculateDatabaseHashTask(List<string> schemaNames) : this() {
            this.SchemaNames = schemaNames;
        }
        public CalculateDatabaseHashTask Calculate() {
            Execute();
            return this;
        }

        public static string Calculate(List<string> schemaNames) => new CalculateDatabaseHashTask(schemaNames).Calculate().DatabaseHash;


    }
}
